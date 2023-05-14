from extract.master_extract import *
from transform.master_transform import *
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

def master_load(df: pd.DataFrame, name_table: str, engine):
    path = './dags/public/dataWarehouse/master'
    df.to_csv(f'{path}/{name_table}.csv', index=False)
    if engine != None:
        df.to_sql(f'{name_table}', engine, if_exists='replace', index=False, chunksize=50000)

@task()
def deployment_table_etl(engine):
    deploy_df = DeploymentTrf.transform_dataframe(DeploymentExt.get_dataframe())
    master_load(deploy_df, 'stg_DeploymentMaster', engine)
    return {'Table(s) processed': 'stg_DeploymentMaster imported successfull'}

@task()
def sku_master_table_etl(engine):
    sku_master_df = SkuMasterTrf.transform_dataframe(SkuMasterExt.get_dataframe())
    master_load(sku_master_df, 'stg_SkuMaster', engine)
    return {'Table(s) processed': 'stg_SkuMaster imported successfull'}

@task()
def location_table_etl(engine):
    location_df = LocationTrf.transform_dataframe(LocationExt.get_dataframe())
    master_load(location_df, 'stg_LocationMaster', engine)
    return {'Table(s) processed': 'stg_LocationMaster imported successfull'}

@task()
def pivotal_size_table_etl(engine):
    pivotal_size_df = PivotalSizeTrf.transform_dataframe(PivotalSizeExt.get_dataframe())
    master_load(pivotal_size_df, 'stg_PivotalSizeMaster', engine)
    return {'Table(s) processed': 'stg_PivotalSizeMaster imported successfull'}

@task()
def seasonality_table_etl(engine):
    seasonality_df = PivotalSizeTrf.transform_dataframe(SeasonalityExt.get_dataframe())
    master_load(seasonality_df, 'stg_SeasonalityMaster', engine)
    return {'Table(s) processed': 'stg_SeasonalityMaster imported successfull'}


default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}
with DAG(
    dag_id='vf_master_etl',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 12),
    catchup=False,
    # tags=
) as dag:
    try:
        # conn = BaseHook.get_connection('vector_db')
        # engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        engine = None   
    except:
        engine = None        

    with TaskGroup("extract_master_load", tooltip="Extract and load source data") as extract_load_src:
        # src_table = deployment_table_etl()
        [deployment_table_etl(engine), sku_master_table_etl(engine), location_table_etl(engine),
         pivotal_size_table_etl(engine), seasonality_table_etl(engine)]
    extract_load_src

