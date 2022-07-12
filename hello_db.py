from datetime import datetime
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


conf_json = {
    "job_id": ,
    "notebook_params": {}
}

def run_databricks_job():
    pass

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    notebook_run = DatabricksRunNowOperator(
        task_id = 'run_notebook',
        json = conf_json
    )

notebook_run