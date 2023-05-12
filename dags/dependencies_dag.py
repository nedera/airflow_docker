from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

def get_pandas():
    import pandas as pd
    print(f'Pandas ver: {pd.__version__}')

with DAG(
    dag_id = 'Ned_dependencies_dag',
    default_args= default_args,
    description = 'This is NederaKUN doing AirFlow with Dependencies',
    start_date = datetime(2023, 5, 12),
    schedule_interval = '@daily',
    
) as dag:
    task1 = PythonOperator(
        task_id = 'get_ver_pandas',
        python_callable=get_pandas
    )
    task1