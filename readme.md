# End-to-End Financial Analytics Pipeline (Lab 02)

## 📌 Project Overview
This project operationalizes a complete **ELT (Extract, Load, Transform)** pipeline for financial data analytics. It integrates **Apache Airflow**, **dbt**, and **Snowflake** to automate the ingestion of stock market data, generate machine learning forecasts, and transform the data for consumption in **Preset (Apache Superset)** dashboards.

The system is fully containerized using Docker, ensuring reproducibility and scalability. It demonstrates a modern data stack architecture capable of handling historical data ingestion, ML inference, and analytical modeling in a unified workflow.

## 🏗️ Architecture

The pipeline follows a decoupled architecture where ingestion and ML training run in parallel, followed by a transformation layer that ensures data quality and lineage.

**Data Flow:**
1.  **Extract:** Airflow pulls raw OHLCV data from Yahoo Finance (`yfinance`) for selected tickers (e.g., AAPL, NVDA).
2.  **Load:** Data is loaded into Snowflake's `RAW` schema.
3.  **Forecast:** A separate Airflow DAG triggers **Snowflake Native ML** functions to generate stock price predictions.
4.  **Transform:** dbt (data build tool) cleans, standardizes, and unions the historical and forecast data into a production-grade Mart.
5.  **Visualize:** Preset dashboards consume the final Mart to display trends and confidence intervals.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow 2.10 (Dockerized)
* **Data Warehouse:** Snowflake
* **Transformation:** dbt Core (v1.8+)
* **Language:** Python 3.12 (Pandas, Snowflake Connector)
* **Visualization:** Preset / Apache Superset
* **Infrastructure:** Docker & Docker Compose

## 📂 Repository Structure

```text
├── dags/
│   ├── market_data_ingest.py    # ETL: Extracts yfinance data -> Snowflake RAW
│   ├── train_predict.py         # ML: Snowflake Cortex forecasting
│   └── elt_dbt_pipeline.py      # ELT: Orchestrates dbt run/test via Sensors
├── dbt/
│   ├── profiles.yml             # Connection profile (copied to container)
│   └── stock_analytics/         # Main dbt project
│       ├── models/
│       │   ├── staging/         # View materializations (cleaning)
│       │   └── marts/           # Table materializations (final logic)
│       └── dbt_project.yml
├── docker-compose.yaml          # Airflow services definition
├── Dockerfile                   # Custom image with dbt & snowflake drivers
├── requirements.txt             # Python dependencies
└── README.md