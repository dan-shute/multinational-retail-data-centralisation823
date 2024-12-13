# This script will contain a class DataCleaning with methods to clean data from each of the sources
import pandas as pd
import numpy as np

class DataCleaning:
    '''
    This class is used to represent a Data Cleansing class. This allows for collected data to be tidied up before being uploaded to one central Database.

    Attributes:
        df (DataFrame): The current DataFrame that is being manipulated and cleansed.
        weight (Series): The weight column of the products data.
    '''
    def __init__(self):
        pass

    def clean_user_data(self, df):
        '''
        Cleans the user data by replacing 'NULL' strings to None datatype, ensuring 'join_date' is a datetime datatype and dropping any NULL values.

        Args:
            df (DataFrame): The user data that was extracted from a database.
        
        Returns:
            DataFrame: The cleansed DataFrame.
        '''
        df = df.replace('NULL', None)
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed', yearfirst = True, errors='coerce')
        df = df.dropna(how = 'any', axis = 0)
        return df
    
    def clean_card_data(self, df):
        '''
        Cleans the card details data. 
        Cleansing rules:
            Replace Null strings with None type.
            Remove non-numeric characters from column 'card_number'.
            Set card_number to be a numeric datatype.
            Set date_payment_confirmed to be a datetime datatype.
            Drop NA values.
        
        Args:
            df (DataFrame): The card details data that was extracted from an S3 Bucket.
        
        Returns:
            DataFrame: The cleansed DataFrame.
        '''
        df = df.replace('NULL', None)
        df['card_number'] = df['card_number'].astype(str).str.replace(r'\D', '', regex = True) # Use regex to remove all non-numeric characters.
        df = df.drop_duplicates(subset= 'card_number', keep= 'last')
        df = df[pd.to_numeric(df['card_number'], errors= 'coerce').notnull()]
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format = 'mixed', yearfirst = True, errors='coerce')
        df = df.dropna(how = 'any', axis = 0)
        return df
        
    def clean_store_data(self, df):
        '''
        Cleans the store data.
        Cleansing Rules:
            Replace Null strings with None type.
            Set opening_date to be date_time datatype.
            Replace newline character representation with a space character, for the address column.
            Remove non-numeric characters within the staff_numbers column.
            Drop NA values.
        
        Args:
            df (DataFrame): The store details data.
        
        Returns:
            DataFrame: The cleansed DataFrame.
        '''
        df = df.replace('NULL', None)
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed', yearfirst = True, errors='coerce')
        df['address'] = df['address'].str.replace('\n', ' ')
        df['staff_numbers'] = df['staff_numbers'].astype(str).str.replace(r'\D', '', regex = True) # Use regex to remove all non-numeric characters.
        df = df.dropna(subset = df.columns.difference(['latitude', 'lat']), how='any', axis = 0) # Drop rows where there is a NULL value, don't look at the 'latitude' column.
        return df

    def kg_convert(self, weight):
        '''
        Converts the weight column of a DataFrame to kg.

        Args:
            weight (Series): Weight column from DataFrame.
        
        Returns:
            float: Floating point value weight represented as kg.
        '''
        if isinstance(weight, float):
            return weight
        elif 'x' in weight:                                            # For values such as '8 x 25g'
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
        '''
        Cleans the products data.
        Cleansing rules:
            Replace the Null strings with None type.
            Convert the weight column to kg using the self.kg_convert() method.
            Drop NA values.

        Args:
            df (DataFrame): The products data that was extracted from an S3 Bucket.
        
        Returns:
            DataFrame: The cleansed data.
        '''
        df = df.replace('NULL', None)
        df['weight'] = df['weight'].apply(self.kg_convert)
        df = df.dropna(how = 'any', axis = 0)
        return df
    
    def clean_orders_data(self, df):
        '''
        Cleans the orders data.
        Cleansing rules:
            Drop the columns 'first_name', 'last_name', '1'.
        
        Args:
            df (DataFrame): The orders data extracted from a database

        Returns:
            DataFrame: The cleansed data.
        '''
        df = df.drop(columns = ['first_name', 'last_name', '1'])
        return df
    
    def clean_events_data(self, df):
        '''
        Cleans the events data.
        Cleansing rules:
            Concatenates the year, month, day columns and converts into a datetime datatype.
            Replace the Null Strings with the None type.
            Drop NA values.

        Args:
            df (DataFrame): The events data extracted from a JSON file.

        Returns:
            DataFrame: The cleansed data.
        '''
        df['date'] = pd.to_datetime(df[['year','month','day']], errors = 'coerce')
        df = df.replace('NULL', None)
        df = df.dropna(how = 'any', axis = 0)
        return df
    