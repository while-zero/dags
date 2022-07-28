from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator


def hello_world(details: str =""):
    print("czesc", details)


for i in range(4):

    dag_id = f"dynamic_dag_no_{i}"
    dag = DAG(
        dag_id=dag_id,
        schedule_interval="*/2 * * * *",
        start_date=datetime(2022, 7, 15),
        catchup=False,
    )

    ingestion_task_id: str = f"ingestion_no_{i}"
    ingestion = PythonOperator(
        task_id=ingestion_task_id,
        python_callable=hello_world,
        op_kwargs={"word": "ingestion dag no {i}"},
    )

    chain(ingestion)

    globals()[dag_id] = dag
