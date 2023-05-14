from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


@task()
def update_1st_grn():
    pass

@task()
def pivotal_size_check():
    pass

@task()
def calc_rosn():
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