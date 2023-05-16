from extract.master_extract import *
from transform.master_transform import *
from common.database import WorkingOnDatabase
from common.data_warehouse import WorkingOnDataWH
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

def master_load(df: pd.DataFrame, name_table: str, is_write_db: bool):
    WorkingOnDataWH().push_dataframe_to_dataWH(df, 'master', name_table)
    if is_write_db:
        WorkingOnDatabase().push_dataframe_to_db(df, name_table)

@task()
def deployment_table_etl():
    deploy_df = DeploymentTrf.transform_dataframe(DeploymentExt.get_dataframe())
    master_load(deploy_df, 'stg_DeploymentMaster', True)
    return {'Table(s) processed': 'stg_DeploymentMaster imported successfull'}

@task()
def sku_master_table_etl():
    sku_master_df = SkuMasterTrf.transform_dataframe(SkuMasterExt.get_dataframe())
    master_load(sku_master_df, 'stg_SkuMaster', True)
    return {'Table(s) processed': 'stg_SkuMaster imported successfull'}

@task()
def location_table_etl():
    location_df = LocationTrf.transform_dataframe(LocationExt.get_dataframe())
    master_load(location_df, 'stg_LocationMaster', True)
    return {'Table(s) processed': 'stg_LocationMaster imported successfull'}

@task()
def pivotal_size_table_etl():
    pivotal_size_df = PivotalSizeTrf.transform_dataframe(PivotalSizeExt.get_dataframe())
    pivotal_size_df.rename(columns={'Min.PivotalSizesForRosn':'MinPSForRosn'}, inplace = True)
    # --- Join PivotalSize columns --- #
    lst = [x.startswith('PivotalSize')
            for x in pivotal_size_df.columns]
    f_pivotal_idx = lst.index(True)
    l_pivotal_idx = len(lst) - 1 - lst[::-1].index(True)
    first4_cols = list(pivotal_size_df.columns[:f_pivotal_idx])
    PS_cols = pivotal_size_df.columns[f_pivotal_idx:l_pivotal_idx]
    
    def join_PS(xs):
        return ','.join([f"'{x}'" for x in xs if pd.notnull(x)])
    pivotal_size_df['PivotalSize'] = pivotal_size_df[PS_cols].apply(
        join_PS, axis=1)
    pivotal_size_df = pivotal_size_df[first4_cols +
                            ['PivotalSize'] + ['MinPSForRosn']]
    master_load(pivotal_size_df, 'stg_PivotalSizeMaster', True)
    return {'Table(s) processed': 'stg_PivotalSizeMaster imported successfull'}

@task()
def seasonality_table_etl():
    seasonality_df = PivotalSizeTrf.transform_dataframe(SeasonalityExt.get_dataframe())
    master_load(seasonality_df, 'stg_SeasonalityMaster', True)
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
    with TaskGroup("extract_master_load", tooltip="Extract and load source data") as extract_load_src:
        # src_table = deployment_table_etl()
        [deployment_table_etl(), sku_master_table_etl(), location_table_etl(),
         pivotal_size_table_etl(), seasonality_table_etl()]
    extract_load_src

