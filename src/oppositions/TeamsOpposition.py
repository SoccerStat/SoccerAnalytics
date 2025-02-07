import pandas as pd

from postgres.PostgresQuerying import PostgresQuerying
from src.oppositions.Oppositions import Oppositions

class TeamsOpposition(Oppositions):
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        
    def build_oppositions(
        self,
        team: str
    ) -> pd.DataFrame:
        
        self.db.execute_sql_file("sql/oppositions/team_x_teams.sql")
        
        return self.db.df_from_query(
            f"""select * 
            from teams_oppositions(
                team := '{team}'
                );""")
        
    def build_matrix(
        self,
        stat: str,
        id_comp: str = 'all',
        season: str = 'all'
    ) -> list:
        
        self.db.execute_sql_file("sql/oppositions/team_x_teams.sql")
        
        cursor_instance = self.db.execute_query_get_cursor(
            f"""select c.complete_name as "Club"
            from club_championship cc
            join club c
            on cc.id_club = c.id
            where {f"id_championship = '{id_comp}'" if id_comp != "all" else "true"}
            and {f"season = '{season}'" if season != "all" else "true"}
            group by c.complete_name
            ;"""
        )
        
        if cursor_instance:
                teams = [row[0] for row in cursor_instance.fetchall()]
                cursor_instance.close()
                df = pd.DataFrame(index=teams, columns=teams)

                for team in teams:
                    cursor_oppositions = self.db.execute_query_get_cursor(
                        f"""select "Team", "Opponent", "{stat}"
                        from teams_oppositions(
                            team := '{team.replace("'", "''")}',
                            id_comp := '{id_comp}',
                            id_season := '{season}'
                            );""")
                    
                    data = cursor_oppositions.fetchall()
                    cursor_oppositions.close()
                    
                    for stats in data:
                        df.loc[stats[0], stats[1]] = stats[2]
                    
                return df

        return pd.DataFrame()