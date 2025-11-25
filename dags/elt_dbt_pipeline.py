from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

DBT_DIR = "/opt/airflow/dbt"
DBT_PROJECT_DIR = f"{DBT_DIR}/stock_analytics"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_dbt_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval='00 3 * * *',
    catchup=False,
    default_args=default_args,
    description="Runs dbt transformations and tests after ETL and ML DAGs complete",
    max_active_runs=1,
) as dag:

    # wait for market_data_ingest (02:30) -> Current run (03:00) = 30 min difference
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_market_data_ingest",
        external_dag_id="market_data_ingest",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        poke_interval=60,
        timeout=60 * 60,
        mode="poke",
        execution_delta=timedelta(minutes=30) # ADDED THIS
    )

    # wait for train_predict (02:45) -> Current run (03:00) = 15 min difference
    wait_for_train = ExternalTaskSensor(
        task_id="wait_for_train_predict",
        external_dag_id="TrainPredict",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        poke_interval=60,
        timeout=60 * 60,
        mode="poke",
        execution_delta=timedelta(minutes=15) # ADDED THIS
    )

   # ... existing imports and sensors ...

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt snapshot --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt debug --profiles-dir {DBT_DIR} && "
            f"dbt run -s mart_stock_analysis --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_mart",
        bash_command=(
            "set -euo pipefail;"
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test -s mart_stock_analysis --profiles-dir {DBT_DIR}"
        ),
    )

    [wait_for_ingest, wait_for_train] >> dbt_snapshot >> dbt_run >> dbt_test