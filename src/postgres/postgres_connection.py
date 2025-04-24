from yaml import load, SafeLoader
import psycopg2


class PostgresConnection:
    """All bout connecting to Postgres database
    """
    def __init__(self, env: str):
        with open(f"conf/{env}.yaml", 'r', encoding='utf-8') as file:
            conf = load(file, Loader=SafeLoader)['postgres']

        self.host = conf["host"]
        self.port = conf["port"]
        self.database = conf["database"]
        self.username = conf["username"]
        self.password = conf["password"]
        self.conn: psycopg2.extensions.connection = None

    def get_conn(self):
        """Get the connection
        """
        return self.conn

    def get_cursor(self) -> psycopg2.extensions.cursor:
        """Get the cursor
        """
        return self.conn.cursor()

    def commit(self):
        """Commit
        """
        return self.conn.commit()

    def rollback(self):
        """Rollback
        """
        return self.conn.rollback()

    def connect(self) -> None:
        """Open the connection
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            print("Connected to PostgreSQL!")
        except psycopg2.Error as e:
            print(f"Error: Could not connect to PostgreSQL. {e}")

    def close(self) -> None:
        """Close the connection
        """
        if self.conn:
            self.conn.close()
            print("Connection to PostgreSQL closed.")
