from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import \
    DatabricksRunNowOperator

conf_json = {
    "job_id": 827734724834405,
    "notebook_params": {
        "cl": "Stefan"
    }
}

with DAG(dag_id="db_dag_stefan",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    notebook_run: DatabricksRunNowOperator = DatabricksRunNowOperator(
        task_id = 'run_notebook',
        json = conf_json
    )

notebook_run
