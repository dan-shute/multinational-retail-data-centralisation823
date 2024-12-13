# This script will contain the class DatabaseConnector.
# It will be used to connect with and upload to the database
import yaml
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class DatabaseConnector:
    '''
    This class is used for interacting with Databases, this could be opening a database connection, or uploading data to a database.

    Attributes:
        df (pandas.DataFrame): A DataFrame object that may be used to upload to a Database.
        table_name (String): The name of a database table.
        engine (sqlalchemy.Engine): A database connection engine.
    '''
    def __init__(self):
        pass
    
    def read_db_creds(self):
        '''
        Reads a YAML file that contains Database credentials into a dictionary.
        
        Returns:
            Dictionary: The credentials stored inside the YAML file. 
        '''
        with open('db_creds.yaml', 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        return data
    
    def init_db_engine(self):
        '''
        Initialises a SQLAlchemy database engine based on the credentials stored inside a YAML file.

        Returns:
            Engine: An initialised SQLAlchemy database connection
        '''
        creds = self.read_db_creds()
        engine = create_engine("postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}".format(**creds), echo = True)
        return engine
    
    def upload_to_db(self, df, table_name, engine):
        '''
        Uploads a DataFrame to a Database table.

        Args:
            df (DataFrame): Data to be uploaded.
            table_name (String): Target database table name.
            engine (Engine): Database connection.
        '''
        df.to_sql(table_name, engine, if_exists = 'replace', index = False)
