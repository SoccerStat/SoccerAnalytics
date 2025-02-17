import pandas as pd

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

        return self.db.df_from_query(
            f"""select * 
            from dwh_utils.players_rankings(
                id_comp := '{id_comp}', 
                id_season := '{season}', 
                first_week := {first_week}, 
                last_week := {last_week},
                side := '{side}'
                );""") #.set_index('Ranking')
