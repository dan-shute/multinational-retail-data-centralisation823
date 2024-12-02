# This script will contain the methods for extracting the data from various sources such as CSV files, an API and an S3 Bucket.
from sqlalchemy import inspect as insp
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self):
        pass

    def list_db_tables(self, engine):
        inspector = insp(engine)
        return inspector.get_table_names()
    
    def read_rds_table(self, db_conn, table_name):
        engine = db_conn.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df

    def retrieve_pdf_data(self, url):
        raw_df_list = tabula.read_pdf(url, pages= 'all')
        df = pd.concat(raw_df_list)
        return df
    
    def api_key(self):
        return {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self, headers, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
        response = requests.get(endpoint, headers=headers)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'):
        df = pd.DataFrame()
        number_of_stores = self.list_number_of_stores(headers=self.api_key())

        for i in range(number_of_stores):
            url = endpoint + f'{i}'
            response = requests.get(url, headers=self.api_key())
            df = pd.concat([df, pd.DataFrame(pd.json_normalize(response.json()))])
        return df

    def extract_from_s3(self, address):
        split_address = address.split('/')
        bucket = split_address[2]
        file_name = split_address[3]
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket = bucket, Key = file_name)
        df = pd.read_csv(obj['Body'])
        return df
        
    def retrieve_events_data(self, address):
        df = pd.read_json(address)
        return df
        