class DeploymentTrf:
    def transform_dataframe(df):
        df['LocationCode'] = df['LocationCode'].astype('str')
        df.drop_duplicates(['LocationCode', 'SkuCode'], keep='first', inplace=True)
        return df

class SkuMasterTrf:
    def transform_dataframe(df):
        df.rename(columns={'CrossPlantCm':'StyleCode'}, inplace=True)
        df.drop_duplicates(['SkuCode'], keep='first', inplace=True)
        return df
    
class LocationTrf:
    def transform_dataframe(df):
        df['LocationCode'] = df['LocationCode'].astype('str')
        df.drop_duplicates(['LocationCode'], keep='first', inplace=True)
        return df
    
class PivotalSizeTrf:
    def transform_dataframe(df):
        df.rename(columns={'Store':'LocationCode'}, inplace=True)
        df.drop_duplicates(['LocationCode', 'Brand', 'SubBrand', 'Category'], keep='first', inplace=True)
        return df
    
class SeasonalityTrf:
    def transform_dataframe(df):
        df.rename(columns={'Store':'LocationCode'}, inplace=True)
        df.drop_duplicates(['LocationCode', 'Brand', 'SubBrand', 'Category'], keep='first', inplace=True)
        return df