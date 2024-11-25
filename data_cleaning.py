# This script will contain a class DataCleaning with methods to clean data from each of the sources
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df = df.replace('NULL', None)
        df = df.dropna(how = 'any', axis = 0)
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed', yearfirst = True, errors='coerce')
        return df