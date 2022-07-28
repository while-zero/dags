from datetime import datetime
from random import getrandbits

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator

# author - Piotrek Janeczek

# example of a dag consisting of two tasks
# funny thing is that the first tasks fails
# in one out of four attemps (on average)

def first_task_with_75_percent() -> None:

    if not getrandbits(2):
        print("huh, unfortunately this time the first task fails.")
        raise AirflowException

    print("first task succeeded. yeah.")

def second_task_always() -> None:
    
    print("i'm in the second task and i'm saying hi!")


with DAG(dag_id="jnk_two_tasks_first_sometimes_fails",
         start_date=datetime(2021,1,1),
         schedule_interval="*/3 * * * *",
         catchup=False) as dag:

    run_first_task = PythonOperator(
        python_callable=first_task_with_75_percent,
        task_id='task_1'
    )
    run_second_task = PythonOperator(
        python_callable=second_task_always,
        task_id='task_2'
    )


    run_second_task << run_first_task
