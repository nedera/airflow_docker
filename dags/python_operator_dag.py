from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}

def passion(who : str, do_things: str):
    # print(f'私の臭味は友達とゲームすることです')
    print(f'私の臭味は{who}と{do_things}ことです')

with DAG(
    dag_id = 'Ned_py_dag_v2',
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
    
    task1