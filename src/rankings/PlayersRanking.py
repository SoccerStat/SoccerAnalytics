from postgres.PostgresToDataFrame import PostgresToDataframe

class PlayersRanking:
    def __init__(self, postgres_to_dataframe: PostgresToDataframe):
        self.db = postgres_to_dataframe
        
    def build_ranking(
        self,
        id_chp: str, 
        season: str, 
        side :str, 
        first_week: int, 
        last_week: int
    ):
        self.db.execute_sql_file("sql/rankings/players.sql")
        
        return self.db.df_from_query(
            f"""select * 
            from players_rankings(
                id_chp := '{id_chp}', 
                id_season := '{season}', 
                side := '{side}', 
                first_week := {first_week}, 
                last_week := {last_week}
                );""").set_index('Ranking')