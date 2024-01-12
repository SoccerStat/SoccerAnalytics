from postgres.PostgresConnection import PostgresConnection
import pandas as pd
import warnings

class PostgresToDataframe:
    def __init__(self, env):
        self.postgres_conn = PostgresConnection(env)
        self.postgres_conn.connect()
        
    def execute_query(self, query):
        if self.postgres_conn.conn:
            return self.postgres_conn.conn.cursor().execute(query)
        
    def execute_sql_file(self, path):
        with open(path, 'r') as sql_file:
            return self.execute_query(sql_file.read())
        
    def df_from_query(self, query):
        if self.postgres_conn.conn:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                return pd.read_sql_query(query, self.postgres_conn.conn)
        else:
            print("No active PostgreSQL connection.")
            return None
        
    def close(self):
        self.postgres_conn.close()