from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from common.data_warehouse import WorkingOnDataWH
from common.database import WorkingOnDatabase
import pandas as pd
from airflow.models import TaskInstance

def uss_load(df: pd.DataFrame, name_table: str, is_write_db: bool):
    WorkingOnDataWH().push_dataframe_to_dataWH(df, 'uss', name_table)
    if is_write_db:
        WorkingOnDatabase().push_dataframe_to_db(df, name_table)

@task()
def update_1st_grn():
    pass

@task()
def pivotal_size_check(ti = None):
    # Extract
    data_conn = WorkingOnDataWH()
    ps_df = data_conn.get_dataframe_from_dataWH('master','stg_PivotalSizeMaster', 
                                                ['LocationCode','Brand', 'SubBrand', 'Category', 'PivotalSize'])
    deploy_df = data_conn.get_dataframe_from_dataWH('master','stg_DeploymentMaster', 
                                                    ['SkuCode', 'LocationCode'])
    sku_master_df = data_conn.get_dataframe_from_dataWH('master','stg_SkuMaster', 
                                                        ['SkuCode', 'StyleCode', 'Brand', 'SubBrand', 'Category', 'Size'])
    # Transform
    ps_df['Brand'] = ps_df['Brand'].map({'Arrow': 'ARR', 'USPA':'USP'})
        

    # Load
    ps_check_df = pd.merge(deploy_df, sku_master_df, how='left', on='SkuCode')
    ps_check_df = pd.merge(ps_check_df, ps_df, how='left', on=['LocationCode', 'Brand', 'SubBrand', 'Category'])
    ps_check_df['PivotalSize'].fillna('', inplace=True) 
    ps_check_df['PivotalSizeCheck'] = [f"'{x[0]}'" in x[1] for x in zip(
            ps_check_df['Size'], ps_check_df['PivotalSize'])]
    
    uss_load(ps_check_df, 'uss_PivotalSizeCheck', True)
    import time
    st = time.time()
    ti.xcom_push(key='ps_check', value= pd.read_csv('dags/public/dataWarehouse/master/stg_SkuMaster.csv').to_dict('list'))
    print(f'>> {time.time() - st}')

    # return{'Table(s) processed': 'uss_PivotalSizeCheck imported successfull'}
    # return ps_check_df.to_dict('list')

@task()
def calc_rosn(ti=None):
    import time
    st = time.time()
    pd.DataFrame(ti.xcom_pull(task_ids ='bpr_etl.pivotal_size_check',key='ps_check'))
    print(f'>> {time.time() - st}')
    pass

@task()
def calc_location_class():
    pass

@task()
def calc_store_transfer():
    pass
default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

with DAG(
    dag_id='vf_user_story_etl',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 15),
    catchup=False,
    # tags=
) as dag:

    with TaskGroup("bpr_etl", tooltip="Make BPR report") as bpr_etl:
        update_grn_date = update_1st_grn()
        ps_check = pivotal_size_check()
        rosn = calc_rosn()

        update_grn_date >> ps_check >> rosn

    with TaskGroup("ist_etl", tooltip="Make IST report") as ist_etl:
        loc_class = calc_location_class()
        ist_report = calc_store_transfer()

        loc_class >> ist_report
    bpr_etl >> ist_etl