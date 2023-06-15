import concurrent.futures
import chardet
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def read_csv_from_s3(url, encoding='utf-8',):
    # # Specify the bucket and file path of the CSV file on S3
    # # Read the CSV file from S3 into a DataFrame
    df = pd.read_csv(url, compression='zip', encoding=encoding)
    # print(f'>> len: {len(df)}')
    return df


@task()
def concatenate_csv_files():
    # Create an S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_s3')

    # Specify the bucket where your CSV files are stored
    bucket = 'vectordata'

    # Specify the folder within the bucket where the CSV files are stored
    folder = 'daily_data/'

    # List all the keys (file paths) in the specified folder
    keys = s3_hook.list_keys(bucket, prefix=folder, delimiter='/')

    # Filter the keys to include only CSV files
    csv_keys = [key for key in keys if key.endswith('.zip')]
    csv_keys = sorted(csv_keys, key=lambda x: (
            datetime.strptime(x.split('_')[-1].replace('.zip', ''), '%d%m%Y') if not x.endswith('InterStoreTransferDataAll.zip') else datetime(2019, 1, 1)
        ))
    csv_keys = csv_keys[-10:]   # Limit files
    # Create a concurrent.futures ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:

        # Submit tasks to read CSV files asynchronously
        futures = [
            executor.submit(read_csv_from_s3, 
                            url=s3_hook.generate_presigned_url(
                                'get_object',
                                params={'Bucket': bucket, 'Key': key},
                                expires_in=300,
                                http_method='GET'
                            ),
                            encoding='utf-8'
            ) for key in csv_keys
        ]

        # Retrieve the results of the tasks
        dfs = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Concatenate DataFrames into a single DataFrame
    concatenated_df = pd.concat(dfs, ignore_index=True)

    # Now you can use the concatenated DataFrame for further processing
    print('------------------------')
    print(f'>>> LEN: {len(concatenated_df)}')

default_args = {
    'owner': 'NederaKUN',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)

}
with DAG(
    dag_id='vf_daily_etl',
    default_args=default_args,
    description='For process daily data',
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 15),
    catchup=False,
    # tags=
) as dag:
    with TaskGroup("extract_daily_load", tooltip="Extract and load source data") as extract_load_src:
        # src_table = deployment_table_etl()
        daily_etl = concatenate_csv_files()
        [daily_etl]
    extract_load_src
