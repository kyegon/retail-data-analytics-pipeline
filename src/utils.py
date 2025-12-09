import os
import psycopg2
import sqlalchemy
import pandas as pd
import urllib.error
import urllib.request
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine,URL

load_dotenv()
def extract_files(url, dst, filename):
    """
     Download and save raw files locally from GitHub. 
     Returns a DataFrame loaded from the downloaded Excel file
     url -- excel document URL
     dst -- destination to save the file
     filename - target member file name
    """
    try:
        dst_path = Path(dst)
        dst_path.mkdir(parents=True, exist_ok=True)
        filepath = dst_path / filename
        print(f"Destination: {filepath}")
        print(f"Downloading from: {url}")
        with urllib.request.urlopen(url) as fin:
            data = fin.read() 
        with open(filepath, mode='wb') as fout:
            fout.write(data)
            print(f"Data size: {len(data):,} bytes ({len(data)/1024/1024:.2f} MB)")
            print(f"Success! File '{filename}' saved to {dst}")
      
        print(f"Loading Excel file into DataFrame...")
        return pd.read_excel(filepath, engine='openpyxl')
        
    except urllib.error.URLError as e:
        print(f"Network Error: Unable to download from {url}")
        print(f"   Details: {e.reason}")
        raise
        
    except FileNotFoundError:
        print(f"File Error: Cannot access {filepath}")
        raise
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")
        raise

class PostgresClient:
    """ The PostgresClient.insert_dataframe method is used to load these Pandas DataFrames directly into a new table in PostgreSQL.
        Define a precise dtype_map to explicitly set database column types (e.g., VARCHAR(100), DATE, NUMERIC(10, 2)).
        This guarantees data integrity and optimizes query performance for all subsequent analytical steps."""
    def __init__(self):
        self.engine = self._get_db_engine()

    def _get_db_engine(self):
        connection_url = URL.create(
            drivername = 'postgresql',
            username = os.getenv('DB_USER'),
            host = os.getenv('DB_HOST'),
            port = os.getenv('DB_PORT'),
            password = os.getenv('DB_PASSWORD'),
            database = os.getenv('DB_NAME')
            )
        return create_engine(connection_url)
    
    def insert_dataframe(self,
            df,
            schema,
            table_name,
            if_exists = 'replace',
            index = False,
            dtype_map = None):
         with self.engine.connect() as connection:
             with connection.begin():
                 try:
                     df.to_sql(con =connection,
                         schema = schema,
                         name = table_name,
                         if_exists = if_exists,
                         index =index,
                         dtype = dtype_map)
                     print(f"Success. Inserted {len(df)} records into {table_name} table")
                 except Exception as e:
                     print(f"An error occured: {e}")
                     raise
    def select_data(self, sql_query):
        with self.engine.connect() as connection:
            try:
                result_df = pd.read_sql(sql_query, connection)
                print(f"Query successful. Fetched {len(result_df)} rows.")
                return result_df
                
            except Exception as e:
                print(f"An Error occured: {e}")
                return pd.DataFrame()
            