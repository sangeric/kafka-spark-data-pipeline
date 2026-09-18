from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.orchestrator import (
    download_api,
    transform_api,
)

with DAG(
    dag_id="binance_api",
    start_date=datetime(2026, 8, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["binance", "api"],
) as dag:

    get_api = PythonOperator(
        task_id="get_binance_api",
        python_callable=download_api,
    )

    transform = PythonOperator(
        task_id="transform_binance_api",
        python_callable=transform_api,
    )

    get_api >> transform