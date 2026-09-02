from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 8, 28),
    schedule=timedelta(minutes=60),
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="date && echo 'Bonjour depuis Airflow'",
    )