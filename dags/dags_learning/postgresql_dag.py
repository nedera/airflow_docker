from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

with DAG(
    dag_id = 'Airflow_with_postgresql_dag_v1',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 11),
) as dag:
    task1 = PostgresOperator(
        task_id = 'count_records_of_IST',
        postgres_conn_id = 'airflow_vector_db',
        sql = '''
        select * from "IST"
        '''
    )
    task1