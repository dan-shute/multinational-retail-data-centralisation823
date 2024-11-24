# This script will contain the class DatabaseConnector.
# It will be used to connect with and upload to the database

import yaml
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class DatabaseConnector:
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        return data
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine("postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}".format(**creds), echo = True)
        return engine
