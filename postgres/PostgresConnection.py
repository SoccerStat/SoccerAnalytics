from yaml import load, SafeLoader
import psycopg2

class PostgresConnection:
    def __init__(self, env):
        with open(f"conf/{env}.yaml", 'r') as file:
            conf = load(file, Loader=SafeLoader)['postgres']
        
        self.host = conf["host"]
        self.port = int(conf["port"])
        self.database = conf["database"]
        self.username = conf["username"]
        self.password = conf["password"]
        self.conn = None
        
    def connect(self):
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
        
    def close(self):
        if self.conn:
            self.conn.close()
            print("Connection to PostgreSQL closed.")