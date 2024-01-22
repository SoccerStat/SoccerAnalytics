import pandas as pd

from postgres.PostgresToDataFrame import PostgresToDataframe

class TeamsOpposition:
    def __init__(self, postgres_to_dataframe: PostgresToDataframe):
        self.db = postgres_to_dataframe
        
    def build_table(
        self,
        team: str
    ) -> pd.DataFrame:
        
        self.db.execute_sql_file("sql/oppositions/teams_x_teams.sql")
        
        return self.db.df_from_query(
            f"""select * 
            from teams_oppositions(
                team := '{team}'
                );""")