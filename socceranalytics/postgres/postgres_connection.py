import psycopg2

from socceranalytics.utils.logging import log

from socceranalytics.data.paths import Config


class PostgresConnection:
    """All bout connecting to Postgres database
    """
    def __init__(self):
        self.host = Config.pg_host
        self.port = Config.pg_port
        self.database = Config.pg_database
        self.username = Config.pg_username
        self.password = Config.pg_password
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
            log("Connected to PostgreSQL!")
        except psycopg2.Error as e:
            log(f"Error: Could not connect to PostgreSQL. {e}", exception=True)

    def close(self) -> None:
        """Close the connection
        """
        if self.conn:
            self.conn.close()
            log("Connection to PostgreSQL closed.")
