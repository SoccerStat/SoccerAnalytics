import pandas as pd
import warnings
from psycopg2.extensions import cursor
from typing import Optional

from postgres.PostgresConnection import PostgresConnection

class PostgresQuerying:
    def __init__(self, env: str):
        self.postgres_conn = PostgresConnection(env)
        self.postgres_conn.connect()

    def execute_query(self, query: str, params=None, return_cursor=False) -> Optional[cursor]:
        if not self.postgres_conn.get_conn():
            return None

        try:
            pg_cursor = self.postgres_conn.get_cursor()
            if params:
                pg_cursor.execute(query, params)
            else:
                pg_cursor.execute(query)
            self.postgres_conn.commit()
            if return_cursor:
                return pg_cursor
        except Exception as e:
            print(f"Error: {e}")
            self.postgres_conn.rollback()

        return None

    def read_sql_file(self, path: str):
        with open(path, 'r', encoding='UTF-8') as sql_file:
            return sql_file.read()

    def execute_sql_file(self, path: str) -> None:
        return self.execute_query(self.read_sql_file(path))

    def df_from_query(self, query: str, params=None) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            cursor_instance = self.execute_query(query, params, return_cursor=True)
            if cursor_instance:
                columns = [desc[0] for desc in cursor_instance.description]
                df = pd.DataFrame(cursor_instance.fetchall(), columns=columns)
                cursor_instance.close()
                return df
            else:
                return pd.DataFrame() #pd.read_sql_query(query, self.postgres_conn.get_conn())

    def close(self) -> None:
        self.postgres_conn.close()
