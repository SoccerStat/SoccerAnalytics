from yaml import load, SafeLoader
from dotenv import load_dotenv
import os


class Config:

    @classmethod
    def load_env_files(cls, env="dev"):
        """Charge d'abord common/env/.env, puis le fichier .env spécifique à l'environnement choisi."""

        # ENV
        cls.ENV = env

        if cls.ENV == "local":
            with open(f"../conf/{env}.yaml", 'r', encoding='utf-8') as file:
                conf = load(file, Loader=SafeLoader)['postgres']

                cls.pg_host = conf["host"]
                cls.pg_port = conf["port"]
                cls.pg_database = conf["database"]
                cls.pg_username = conf["username"]
                cls.pg_password = conf["password"]
                cls.PATH_LOGGING = None
        else:
            common_env_path = os.path.abspath("common/env/.env")
            load_dotenv(common_env_path, override=True)

            env_path = os.path.abspath(f"{env}/env/.env")
            load_dotenv(env_path, override=True)

            cls.pg_host = os.environ.get("PG_HOST")
            cls.pg_port = os.environ.get("PG_PORT")
            cls.pg_database = os.environ.get("PG_DATABASE")
            cls.pg_username = os.environ.get("PG_USERNAME")
            cls.pg_password = os.environ.get("PG_PASSWORD")

            # LOGGING
            cls.PATH_LOGGING = os.environ.get("PATH_LOGGING")
