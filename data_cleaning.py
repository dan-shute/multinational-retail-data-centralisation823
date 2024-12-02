# This script will contain a class DataCleaning with methods to clean data from each of the sources
import pandas as pd
import numpy as np

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
        
    def clean_store_data(self, df):
        df = df.replace('NULL', None)
        df = df.drop(columns = ['lat'], axis=1)
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed', yearfirst = True, errors='coerce')
        df['address'] = df['address'].str.replace('\n', ' ')
        df['staff_numbers'] = df['staff_numbers'].astype(str).str.extract(r'(\d+\.*\d*)')
        df = df.dropna(subset = df.columns.difference(['latitude']), how='any', axis = 0)
        return df
    
    def convert_product_weights(self, df):
        df['weight'] = df['weight'].apply(self.kg_convert)
        return df

    def kg_convert(self, weight):
        if isinstance(weight, float):
            return weight
        elif 'x' in weight:
            factor = int(weight.split('x')[0])
            multiplicand = float(weight.split('x')[1].replace('g',''))
            return (factor * multiplicand) / 1000
        elif weight.endswith('.'):
            return float(weight.split(' ')[0].replace('g', ''))
        elif 'ml' in weight:
            return float(weight.replace('ml', '')) / 1000
        elif 'kg' in weight:
            return float(weight.replace('kg', ''))
        elif 'g' in weight:
            return float(weight.replace('g', '')) / 1000
        elif 'oz' in weight:
            return round(float(weight.replace('oz', '')) / 35.274, 2)
        else:
            return np.nan

    def clean_products_data(self, df):
        df = df.replace('NULL', None)
        df['weight'] = df['weight'].apply(self.kg_convert)
        df = df.dropna(how = 'any', axis = 0)
        return df
    