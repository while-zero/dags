from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator


def hello_world(details: str =""):
    print("czesc", details)


def create_dag(symbol:str) -> DAG:
    dag_id = f"dynamic_dag_no_{symbol}"
    
    with DAG(
        dag_id=dag_id,
        schedule_interval="*/2 * * * *",
        start_date=datetime(2022, 7, 15),
        catchup=False,
    ) as dag:

        ingestion_task_id: str = f"ingestion_no_{symbol}"
        ingestion = PythonOperator(
            task_id=ingestion_task_id,
            python_callable=hello_world,
            op_kwargs={"word": "ingestion dag no {symbol}"},
        )

        chain(ingestion)

    return dag

for element in ['jeden', 'dwa', 'trzy']:
    globals()[element] = create_dag(symbol=element)
