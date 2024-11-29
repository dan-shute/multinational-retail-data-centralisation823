# This script will contain a class DataCleaning with methods to clean data from each of the sources
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        df = df.replace('NULL', None)
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed', yearfirst = True, errors='coerce')
        df = df.dropna(how = 'any', axis = 0)
        return df
    
    def clean_card_data(self, df):
        df = df.replace('NULL', None)
        df['card_number'] = df['card_number'].astype(str).str.extract(r'(\d+\.*\d*)')
        df = df.drop_duplicates(subset= 'card_number', keep= 'last')
        df = df[pd.to_numeric(df['card_number'], errors= 'coerce').notnull()]
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format = 'mixed', yearfirst = True, errors='coerce')
        df = df.dropna(how = 'any', axis = 0)
        return df
        
    
    