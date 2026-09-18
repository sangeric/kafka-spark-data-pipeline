from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from src.orchestrator import download_batch

with DAG(
    dag_id="binance_batch",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    download = PythonOperator(
        task_id="download_binance_batch",
        python_callable=download_batch,
    )

    transform = BashOperator(
        task_id="transform_binance_batch",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/project/src/orchestrator.py batch
        """
    )

    download >> transform