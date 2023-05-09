from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_arg = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)

}
with DAG(
    dag_id = 'Ned_dag_v5',
    default_args= default_arg,
    description = 'This is NederaKUN doing AirFlow with BashOperator',
    start_date = datetime(2023, 5, 8, 2),
    schedule_interval = '@daily',
    
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 私はねでらと申します。"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo FPT 大学"
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command= 'echo 23歳です'
    )
    # task1 >> task2
    # task1 >> task3
    task1 >> [task2, task3]