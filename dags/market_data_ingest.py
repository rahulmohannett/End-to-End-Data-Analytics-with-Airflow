# dags/market_data_ingest.py
# Full refresh OHLCV from yfinance into USER_DB_OSTRICH.RAW.MARKET_DATA
# - Single-task load (no dynamic mapping) to avoid map_index = -1
# - TRUE full refresh: TRUNCATE RAW, then INSERT a de-duplicated SELECT from STAGE
# - LOW/HIGH column names (no reserved words)
# - DQ prints sample duplicate keys if any are detected

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List
import logging
import time

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# ----------------------------
# Config via Airflow Variables
# ----------------------------
DB_NAME: str = Variable.get("db_name", "USER_DB_OSTRICH")
RAW_SCHEMA: str = Variable.get("raw_schema", "RAW")
SYMBOLS: List[str] = [s.strip().upper() for s in Variable.get("symbols", "AAPL,NVDA").split(",") if s.strip()]
TRAIN_DAYS: int = int(Variable.get("train_days", "180"))

RAW_TABLE = "MARKET_DATA"
STAGE_TABLE = "MARKET_DATA_STAGE"

logger = logging.getLogger("airflow.task")

def fq(obj: str) -> str:
    """Return 3-part, quoted identifier for DB objects."""
    return f'"{DB_NAME}"."{RAW_SCHEMA}"."{obj}"'

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="market_data_ingest",
    description="Full refresh yfinance OHLCV into RAW.MARKET_DATA (LOW/HIGH columns)",
    start_date=datetime(2025, 11, 1),
    schedule='30 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "yfinance", "snowflake", "full-refresh"],
) as dag:
    # -------------------------------------------------------
    # DDL: ensure schema & tables exist (idempotent)
    # -------------------------------------------------------
    ensure_objects = SnowflakeOperator(
        task_id="ensure_objects",
        snowflake_conn_id="snowflake_conn",
        sql=[
            f'create schema if not exists "{DB_NAME}"."{RAW_SCHEMA}"',
            f"""
            create table if not exists {fq(RAW_TABLE)} (
                SYMBOL string,
                DATE date,
                OPEN float,
                CLOSE float,
                LOW float,
                HIGH float,
                VOLUME number
            )
            """,
            f"""
            create table if not exists {fq(STAGE_TABLE)} (
                SYMBOL string,
                DATE date,
                OPEN float,
                CLOSE float,
                LOW float,
                HIGH float,
                VOLUME number
            )
            """,
        ],
    )

    # ---------------------------
    # Empty STAGE at each run
    # ---------------------------
    truncate_stage = SnowflakeOperator(
        task_id="truncate_stage",
        snowflake_conn_id="snowflake_conn",
        sql=f"truncate table if exists {fq(STAGE_TABLE)}",
    )

    # ----------------------------------------------
    # Extract+Transform all symbols -> write STAGE
    # (single Python task; no dynamic mapping)
    # ----------------------------------------------
    def load_all_symbols_to_stage(train_days: int = TRAIN_DAYS) -> None:
        if not SYMBOLS:
            raise ValueError("Airflow Variable 'symbols' resolved to an empty list.")

        frames = []
        logger.info(f"[load_all] symbols={SYMBOLS} train_days={train_days}")

        for symbol in SYMBOLS:
            # keep raw OHLCV; yfinance changed defaults
            retries = 3
            last_err = None
            df = None
            for attempt in range(1, retries + 1):
                try:
                    df = yf.download(
                        symbol,
                        period=f"{train_days}d",
                        interval="1d",
                        group_by="column",   # avoid MultiIndex like ('Close','AAPL')
                        auto_adjust=False,   # keep raw OHLC values deterministic
                        threads=False,
                        progress=False,
                    )
                    if df is not None and not df.empty:
                        break
                    last_err = RuntimeError(f"Empty dataframe for {symbol}")
                except Exception as e:
                    last_err = e
                time.sleep(2)
            if df is None or df.empty:
                raise last_err or RuntimeError(f"No data for {symbol}")

            # If a MultiIndex sneaks in anyway, flatten
            if isinstance(df.columns, pd.MultiIndex):
                if symbol in df.columns.get_level_values(-1):
                    df = df.xs(symbol, axis=1, level=-1, drop_level=True)
                else:
                    df.columns = df.columns.get_level_values(0)

            df = df.reset_index()

            expected = {"Date", "Open", "High", "Low", "Close", "Volume"}
            missing = expected.difference(df.columns)
            if missing:
                raise ValueError(f"{symbol}: missing columns {missing}. Got: {list(df.columns)}")

            # Normalize to target schema
            df = df.rename(
                columns={
                    "Date": "DATE",
                    "Open": "OPEN",
                    "High": "HIGH",
                    "Low": "LOW",
                    "Close": "CLOSE",
                    "Volume": "VOLUME",
                }
            )
            df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

            # Numerics & cleanup
            for col in ["OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]:
                df[col] = pd.to_numeric(df.get(col), errors="coerce")

            df["SYMBOL"] = symbol.upper()
            df = df[["SYMBOL", "DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]]

            # Drop nulls on key & obvious dupes within this symbol
            before = len(df)
            df = df.dropna(subset=["DATE"]).drop_duplicates(subset=["SYMBOL", "DATE"])
            logger.info(f"[load_all] {symbol}: rows before={before} after={len(df)}")

            frames.append(df)

        all_df = pd.concat(frames, ignore_index=True)
        if all_df.empty:
            raise RuntimeError("Nothing to load to STAGE.")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn:
            logger.info(f"[load_all] write_pandas -> {DB_NAME}.{RAW_SCHEMA}.{STAGE_TABLE}")
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=all_df,
                table_name=STAGE_TABLE,
                database=DB_NAME,
                schema=RAW_SCHEMA,
                quote_identifiers=True,
                overwrite=False,  # STAGE already truncated above
            )
        if not success or nrows == 0:
            raise RuntimeError(f"write_pandas failed (success={success}, rows={nrows})")
        logger.info(f"[load_all] staged rows={nrows} chunks={nchunks}")

    load_stage = PythonOperator(
        task_id="load_stage_full",
        python_callable=load_all_symbols_to_stage,
        execution_timeout=timedelta(minutes=5),
        retries=1,
        retry_delay=timedelta(seconds=20),
    )

    # -----------------------------------------
    # TRUE full refresh of RAW from STAGE
    #  - TRUNCATE RAW
    #  - INSERT de-duped rows from STAGE (1 row per SYMBOL, DATE)
    # -----------------------------------------
    full_refresh = SnowflakeOperator(
        task_id="full_refresh",
        snowflake_conn_id="snowflake_conn",
        sql=[
            f"truncate table {fq(RAW_TABLE)}",
            # Use QUALIFY/ROW_NUMBER to ensure only one row per (SYMBOL, DATE)
            f"""
            insert into {fq(RAW_TABLE)} (SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME)
            select SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME
            from {fq(STAGE_TABLE)}
            qualify row_number() over (partition by SYMBOL, DATE order by DATE) = 1
            """,
        ],
    )

    # -------------------------
    # DQ: no duplicates (verbose)
    # -------------------------
    def dq_no_dupes() -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        with hook.get_conn() as conn, conn.cursor() as cur:
            # Check duplicates
            q_count = f"""
                select count(*) from (
                  select SYMBOL, DATE, count(*) c
                  from {fq(RAW_TABLE)}
                  group by 1,2
                  having count(*) > 1
                )
            """
            cur.execute(q_count)
            dupes = cur.fetchone()[0]

            if dupes and dupes > 0:
                # Print a few offending keys to logs for quick triage
                q_sample = f"""
                    select SYMBOL, DATE, count(*) c
                    from {fq(RAW_TABLE)}
                    group by 1,2
                    having count(*) > 1
                    order by c desc, SYMBOL, DATE
                    limit 10
                """
                cur.execute(q_sample)
                rows = cur.fetchall()
                msg_lines = [f"{r[0]}|{r[1]} -> {r[2]} rows" for r in rows]
                raise AssertionError(
                    "DQ failed: duplicate (SYMBOL,DATE) rows found in RAW.\n"
                    + "\n".join(msg_lines)
                )

            # Optional: sanity ratios to logs
            cur.execute(f"select count(*) from {fq(RAW_TABLE)}")
            total = cur.fetchone()[0]
            cur.execute(f"select count(distinct SYMBOL||'|'||DATE) from {fq(RAW_TABLE)}")
            distinct_pairs = cur.fetchone()[0]
            print(f"[DQ] total_rows={total} distinct_pairs={distinct_pairs}")

    dq_check = PythonOperator(task_id="dq_no_dupes", python_callable=dq_no_dupes)

    # -----------------
    # Task wiring
    # -----------------
    ensure_objects >> truncate_stage >> load_stage >> full_refresh >> dq_check