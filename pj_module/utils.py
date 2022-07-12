from airflow import DAG

def create_dag(dag_id: str,
               schedule: str,
               dag_number,
               default_args,
               tasks) -> DAG:

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        task1 = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_py)

    return dag


number_of_dags = Variable.get('dag_number', default_var=3)
number_of_dags = int(number_of_dags)

for n in range(1, number_of_dags):
    dag_id = 'hello_world_{}'.format(str(n))

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    schedule = '@daily'
    dag_number = n
    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)
