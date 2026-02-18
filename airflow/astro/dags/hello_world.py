from pendulum import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    start_date=datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
)
def hello_world_dag():
    BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello, World from Airflow!'",
    )


hello_world_dag()
