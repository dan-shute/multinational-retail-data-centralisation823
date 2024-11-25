# This script will contain the methods for extracting the data from various sources such as CSV files, an API and an S3 Bucket.
from sqlalchemy import inspect as insp
import pandas as pd



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



    