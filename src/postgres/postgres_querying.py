import traceback
from typing import Optional
import warnings
from psycopg2.extensions import cursor
from psycopg2 import OperationalError, DatabaseError
import pandas as pd

from src.postgres.postgres_connection import PostgresConnection


class PostgresQuerying:
    """All about querying the database
    """
    def __init__(self, env: str):
        self.postgres_conn = PostgresConnection(env)
        self.postgres_conn.connect()

    def execute_query(self, query: str, params=None, return_cursor=False) -> Optional[cursor]:
        """Execute a query with or without parameters
        """
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
        except OperationalError as oe:
            print(f"Operational Error: {oe}")
            traceback.print_exc()
            self.postgres_conn.rollback()  # Annule toutes les modifications sur une erreur de connexion
        except DatabaseError as de:
            print(f"Database Error: {de}")
            traceback.print_exc()
            self.postgres_conn.rollback()  # Annule toutes les modifications en cas d'erreur de base de données
        except Exception as e:
            print(f"General Error: {e}")
            traceback.print_exc()
            self.postgres_conn.rollback()

        return None

    def read_sql_file(self, path: str):
        """Read a SQL file
        """
        with open(path, 'r', encoding='UTF-8') as sql_file:
            return sql_file.read()

    def execute_sql_file(self, path: str) -> None:
        """Execute a query stored in a file
        """
        return self.execute_query(self.read_sql_file(path))

    def df_from_query(self, query: str, params=None) -> pd.DataFrame:
        """Convert the result of a query into a pandas DataFrame
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            cursor_instance = self.execute_query(query, params, return_cursor=True)
            if cursor_instance:
                columns = [desc[0] for desc in cursor_instance.description]
                df = pd.DataFrame(cursor_instance.fetchall(), columns=columns)
                cursor_instance.close()
                return df
            else:
                return pd.DataFrame()  # pd.read_sql_query(query, self.postgres_conn.get_conn())

    def close(self) -> None:
        """Close the connection to Postgres
        """
        self.postgres_conn.close()
