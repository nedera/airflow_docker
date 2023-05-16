import pandas as pd
class WorkingOnDataWH:
    def __init__(self) -> None:
        self.path = './dags/public/dataWarehouse'
    
    def get_dataframe_from_dataWH(self, folder, file_name, usecols:list = None):
        try:
            return pd.read_csv(f'{self.path}/{folder}/{file_name}.csv', usecols=usecols)
        except Exception as e:
            print(f'Failed get DF: {e}')
    
    def push_dataframe_to_dataWH(self, df: pd.DataFrame, folder, file_name):
        try:
            df.to_csv(f'{self.path}/{folder}/{file_name}.csv', index=False)
            print(f'Created {file_name} sucessfully on dataWH')

        except Exception as e:
            print(f'Failed push dataWH: {e}')