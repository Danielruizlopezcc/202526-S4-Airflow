from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_01_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hola desde Airflow"',
    )

    end = EmptyOperator(task_id="end")

    start >> hello >> end