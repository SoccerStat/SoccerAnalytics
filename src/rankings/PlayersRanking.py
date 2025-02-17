import pandas as pd
from psycopg2 import sql

from postgres.PostgresQuerying import PostgresQuerying

class PlayersRanking:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe

    def build_ranking(
        self,
        id_comp: str,
        season: str,
        first_week: int = 1,
        last_week: int = 100,
        side:str = 'both'
    ) -> pd.DataFrame:

        season = season.replace('-', '_')

        self.db.execute_sql_file("sql/rankings/sub_functions.sql")
        self.db.execute_sql_file("sql/rankings/players.sql")

        query = sql.SQL("""
            select * 
            from dwh_utils.players_rankings(
                id_comp := %s, 
                id_season := %s,
                first_week := %s,
                last_week := %s,
                side := %s
            );
        """) #.set_index('Ranking')

        return self.db.df_from_query(query, (id_comp, season, first_week, last_week, side))
