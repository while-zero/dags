from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

conf_json = {
    "notebook_task": {
        "notebook_path": "/Shared/some_db_notebook",
        "notebook_params": {
            "input": "xyz",
        }
    },
    "existing_cluster_id": "jnk_test",
    "run_name": "yyy"
}

with DAG(
    dag_id="databricks_using_DatabricksSubmitRunOperator",
    start_date=datetime(2022,8,1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["databricks"],
) as dag:
    first_task_in_dag = DatabricksSubmitRunOperator(
        task_id="task_1_db_submit_job",
        json=conf_json
    )

    first_task_in_dag