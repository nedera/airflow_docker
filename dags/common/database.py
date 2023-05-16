from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook
import pandas as pd

class WorkingOnDatabase:
    def __init__(self) -> None:
        self.conn = BaseHook.get_connection('vector_db')
        self.engine = create_engine(f'postgresql://{self.conn.login}:{self.conn.password}@{self.conn.host}:{self.conn.port}/{self.conn.schema}')
    def get_dataframe_from_db(self, query):
        try:
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            print(f'Failed get DF: {e}')
    
    def push_dataframe_to_db(self, df: pd.DataFrame ,table_name):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False, chunksize=50000)
            print(f'Created {table_name} sucessfully on Database')

        except Exception as e:
            print(f'Failed push db: {e}')

        

