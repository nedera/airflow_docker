from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

@dag(dag_id='Ned_api_dag_v3', 
     default_args= default_args,
     start_date= datetime(2023, 5, 1),
     schedule_interval='0 9 * * 1-5',
    #  catchup=True
)
def self_introduction():
    @task(multiple_outputs=True)
    def get_name():
        return {'fname':'ねでら',
                'lname': '阮'
                }
    
    @task()
    def get_age():
        return 22
    
    @task()
    def greet(fname,lname, age):

        print(f'はじめまして。{fname}{lname}と申します、{age}歳です' )

    name_dict = get_name()
    fname = name_dict['fname']    
    lname = name_dict['lname']    
    greet(fname, lname, age=get_age())

greet_dag = self_introduction()