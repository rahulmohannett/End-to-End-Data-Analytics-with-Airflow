FROM apache/airflow:2.10.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && apt-get clean

USER airflow

# 1. Install Airflow-compatible libraries in the MAIN environment
# We remove dbt from here to stop the conflict.
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    yfinance

# 2. Install dbt in a SEPARATE virtual environment
# This isolates dbt's heavy dependencies from Airflow's core.
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate

# 3. Add the dbt virtual environment to the system PATH
# This ensures that when you type 'dbt run', it uses the isolated version.
ENV PATH="/opt/airflow/dbt_venv/bin:$PATH"