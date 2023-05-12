import time

from common.master_processing import rename_columns 
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine


MASTER_DATA = {
    'Ddeployment Master': [],
    'SKUMaster': [],
    'LocationMaster': [],
    'PivotalSizeMaster': [],
    'Seasonality Master': [],
}

# Extract tasks
@task()
def get_src_table():
    import os
    print(f'Hello: >>{os.getcwd()}')
    path = '/opt/airflow/dags/public/dataLake/master'
    for table in MASTER_DATA.keys():

        df = pd.read_csv(f'{path}/{table}.csv')
        print('===+++===')
        print(df.columns)
        df = rename_columns(df)
        print(df.columns)
        print('===+++===')

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}
with DAG(
    dag_id='vector_flow',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 12),
    catchup=False,
    # tags=
) as dag:
    with TaskGroup("extract_master_load", tooltip="Extract and load source data") as extract_load_src:
        src_table = get_src_table()
        [src_table]
    extract_load_src

