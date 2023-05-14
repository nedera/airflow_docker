from common.master_processing import rename_cols
import pandas as pd

PATH = '/opt/airflow/dags/public/dataLake/master'
MASTER_DATA = [
    'Ddeployment Master',
    'SKUMaster',
    'LocationMaster',
    'PivotalSizeMaster',
    'Seasonality Master',
]

class DeploymentExt:
    def get_dataframe():
        df = pd.read_csv(f'{PATH}/{MASTER_DATA[0]}.csv', usecols=lambda x: not x.startswith('Unnamed:'), skipinitialspace=True)
        df = df.rename(columns=rename_cols)
        return df

class SkuMasterExt:
    def get_dataframe():
        df = pd.read_csv(f'{PATH}/{MASTER_DATA[1]}.csv', usecols=lambda x: not x.startswith('Unnamed:'), skipinitialspace=True)
        df = df.rename(columns=rename_cols)
        return df
    
class LocationExt:
    def get_dataframe():
        df = pd.read_csv(f'{PATH}/{MASTER_DATA[2]}.csv', usecols=lambda x: not x.startswith('Unnamed:'), skipinitialspace=True)
        df = df.rename(columns=rename_cols)
        return df
    
class PivotalSizeExt:
    def get_dataframe():
        df = pd.read_csv(f'{PATH}/{MASTER_DATA[3]}.csv', usecols=lambda x: not x.startswith('Unnamed:'), skipinitialspace=True)
        df = df.rename(columns=rename_cols)
        return df
    
class SeasonalityExt:
    def get_dataframe():
        df = pd.read_csv(f'{PATH}/{MASTER_DATA[4]}.csv', usecols=lambda x: not x.startswith('Unnamed:'), skipinitialspace=True)
        df = df.rename(columns=rename_cols)
        return df