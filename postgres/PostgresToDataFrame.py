import pandas as pd
import warnings
from psycopg2.extensions import cursor
from typing import Optional

from postgres.PostgresConnection import PostgresConnection

class PostgresToDataframe:
    def __init__(self, env: str):
        self.postgres_conn = PostgresConnection(env)
        self.postgres_conn.connect()
        
    def execute_query_get_cursor(self, query: str) -> Optional[cursor]:
        if not self.postgres_conn.get_conn():
            return None
        
        try:
            cursor = self.postgres_conn.get_cursor()
            cursor.execute(query)
            return cursor
        except Exception as e:
            print(f"Error: {e}")
            return None
        
    def execute_sql_file(self, path: str) -> None:
        with open(path, 'r') as sql_file:
            return self.execute_query_get_cursor(sql_file.read())
        
    def df_from_query(self, query: str) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            
            cursor_instance = self.execute_query_get_cursor(query)
            if cursor_instance:
                columns = [desc[0] for desc in cursor_instance.description]
                df = pd.DataFrame(cursor_instance.fetchall(), columns=columns)
                cursor_instance.close()
                return df
            else:
                return pd.DataFrame() #pd.read_sql_query(query, self.postgres_conn.get_conn())
        
    def close(self) -> None:
        self.postgres_conn.close()