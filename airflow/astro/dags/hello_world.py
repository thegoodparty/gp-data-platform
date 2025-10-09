import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_world_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello, World from Airflow!'",
    )
