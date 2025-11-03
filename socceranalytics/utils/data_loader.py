from psycopg2 import sql

from socceranalytics.postgres.postgres_querying import PostgresQuerying


class DataLoader:
    """Common object for ranking and opposition objects.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        # self.utils_sql_path = "socceranalytics.sql.utils"

        # self.db.execute_sql_file(self.utils_sql_path, "schemas.sql")
        # self.db.execute_sql_file(self.utils_sql_path, "types.sql")
        # self.db.execute_sql_file(self.utils_sql_path, "checks.sql")
        # self.db.execute_sql_file(self.utils_sql_path, "aggregations.sql")
        # self.db.execute_sql_file(self.utils_sql_path, "competitions.sql")

    def get_seasons(self):
        """Get all the season schemas.
        """
        all_season_schemas_query = sql.SQL("""select * from analytics.get_season_schemas();""")
        all_seasons_schemas = self.db.df_from_query(all_season_schemas_query).iloc[:, 0].tolist()
        return [season_schema[7:] for season_schema in all_seasons_schemas if season_schema[7:] >= "2025_2026"]

    def get_competition_ids(self):
        """Get the interesting competitions (domestic cups excluded).
        """
        all_comps_query = sql.SQL("""select * from analytics.get_competition_ids();""")
        return self.db.df_from_query(all_comps_query).iloc[:, 0].tolist()

    def get_competition_names(self):
        """Get the interesting competitions (domestic cups excluded).
        """
        all_comps_query = sql.SQL("""select * from analytics.get_competition_names();""")
        return self.db.df_from_query(all_comps_query).iloc[:, 0].tolist()
