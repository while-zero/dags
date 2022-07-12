from datetime import datetime
# from typing import Optional

from airflow import DAG

from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator





from pj_module.dag_configuration import client_configuration
for client_name, client_config in client_configuration.items():
