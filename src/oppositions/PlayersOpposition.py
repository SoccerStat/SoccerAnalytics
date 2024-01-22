import pandas as pd

from postgres.PostgresToDataFrame import PostgresToDataframe

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresToDataframe):
            self.db = postgres_to_dataframe
        
    def build_table(
        self,
        player: str
    ) -> pd.DataFrame:
        
        self.db.execute_sql_file("sql/oppositions/players_x_teams.sql")
        return self.db.df_from_query(
            f"""select * 
            from players_oppositions(
                player := '{player}'
                );""")