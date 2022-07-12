from datetime import datetime
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_world(**kwargs):
    word: Optional[str] = kwargs.get('word', None)
    print("Hello World", word)

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    task1 = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    op_kwargs={"word":'taskpierwszy'})

    task2 = PythonOperator(
    task_id="hello_world2",
    python_callable=hello_world,
    op_kwargs={"word":'taskdrugi'})

task1 >> task2
