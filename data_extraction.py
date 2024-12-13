# This script will contain the methods for extracting the data from various sources such as CSV files, an API and an S3 Bucket.
from sqlalchemy import inspect as insp
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    '''
    This class is used for DataExtraction from multiple sources. It allows for easy reading of data and aims to read the data into pandas DataFrames

    Attributes:
        engine (sqlalchemy.Engine): A database connection engine.
        db_conn (DatabaseConnector()): A DatabaseConnector object.
        table_name (String): A database table name.
        url (String): A URL that hosts data.
        headers (Dictionary): Headers used to interact with an API.
        endpoint (String): An AWS endpoint for interacting with files stored on AWS.
        address (String): A URL for AWS files within s3 buckets.
    '''
    def __init__(self):
        pass

    def list_db_tables(self, engine):
        '''
        Lists all tables from a database.

        Args:
            engine (Engine): The database connection engine to read the list of tables from.
        
        Returns:
            List: A list of all the tables in the database.
        '''
        inspector = insp(engine)
        return inspector.get_table_names()
    
    def read_rds_table(self, db_conn, table_name):
        '''
        Reads the data from a target database table into a DataFrame.

        Args:
            db_conn (DatabaseConnector()): The Database Connection.
            table_name (String): The target table name.

        Returns:
            DataFrame: A pandas.DataFrame of the entire database table.
        '''
        engine = db_conn.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df

    def retrieve_pdf_data(self, url):
        '''
        Reads data from a pdf into a DataFrame.

        Args:
            url (String): The target url which hosts the pdf file.

        Returns:
            DataFrame: A pandas.DataFrame of all pdf pages at the URL.
        '''
        raw_df_list = tabula.read_pdf(url, pages= 'all')
        df = pd.concat(raw_df_list)
        return df
    
    def api_key(self):
        '''
        Returns:
            Dictionary: A key-value pair in which the value is an API Key required for interacting with an API.
        '''
        return {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self, headers, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
        '''
        Retrieve the number of stores stored within AWS via interacting with an API.

        Args:
            endpoint (String): The AWS endpoint.
            headers (Dictionary): The API Key.

        Returns:
            String: The number of stores. 
        '''
        response = requests.get(endpoint, headers=headers)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'):
        '''
        Gets the data for each store in AWS.

        Args:
            endpoint (String): The AWS endpoint.

        Returns:
            DataFrame: The DataFrame object of all the stores information concatenated.
        '''
        df = pd.DataFrame()
        number_of_stores = self.list_number_of_stores(headers=self.api_key())

        for i in range(number_of_stores):
            url = endpoint + f'{i}'
            response = requests.get(url, headers=self.api_key())
            df = pd.concat([df, pd.DataFrame(pd.json_normalize(response.json()))])
        return df

    def extract_from_s3(self, address):
        '''
        Reads a csv file stored in an AWS s3 bucket into a DataFrame.

        Args: 
            address (String): The url address of the csv file in the s3 bucket.

        Returns:
            DataFrame: The DataFrame object of the csv file stored within the s3 bucket. 
        '''
        split_address = address.split('/')
        bucket = split_address[2]
        file_name = split_address[3]
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket = bucket, Key = file_name)
        df = pd.read_csv(obj['Body'])
        return df
        
    def retrieve_events_data(self, address):
        '''
        Reads a json file stored in an s3 bucket.

        Args:
            address (String): The url address of the json file in the s3 bucket.

        Returns:
            DataFrame: The DataFrame object of the json file stored within the s3 bucket.
        '''
        df = pd.read_json(address)
        return df
        