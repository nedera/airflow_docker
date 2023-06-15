from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

def passion(who : str, do_things: str, ti):
    fname = ti.xcom_pull(task_ids = 'friends', key='fname')
    lname = ti.xcom_pull(task_ids = 'friends',key='lname')
    age = ti.xcom_pull(task_ids = 'friends_age',key='age')

    print(f'私の臭味は{who}と{do_things}ことです')
    print(f'私の友達は{fname}{lname}、{age}歳です')

def friends(ti):
    fname = ti.xcom_push(key='fname', value = 'さら')
    lname = ti.xcom_push(key='lname', value = '長野')

def friends_age(ti):
    age = ti.xcom_push(key='age', value = 19)

with DAG(
    dag_id = 'Ned_py_dag_v4',
    default_args=default_args,
    description='This is NederaKUN doing AirFlow with PythonOperator',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 9, 2),
) as dag:
    task1 = PythonOperator(
        task_id="syuumi_1",
        python_callable=passion,
        op_kwargs={'who':'友達', 'do_things':'ゲームする'}
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task2 = PythonOperator(
        task_id="friends",
        python_callable=friends,

    )

    task3 = PythonOperator(
        task_id="friends_age",
        python_callable=friends_age,

    )
    
    [task2, task3] >> task1