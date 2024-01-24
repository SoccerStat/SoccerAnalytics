import pandas as pd

from postgres.PostgresQuerying import PostgresQuerying
from src.oppositions.Oppositions import Oppositions

class PlayersOpposition(Oppositions):
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        
    def build_oppositions(
        self,
        player: str
    ) -> pd.DataFrame:
        
        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")
        
        return self.db.df_from_query(
            f"""select * 
            from players_oppositions(
                player := '{player}'
                );""")
        
    def build_matrix(
        self,
        stat: str,
        id_chp: str = 'all',
        season: str = 'all'
    ) -> pd.DataFrame:
        
        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")
        
        cursor_players = self.db.execute_query_get_cursor(
            f"""select p.name as "Player"
            from player p
            join player_club pc
            on pc.id_player = p.id
            join club_championship cc
            on pc.id_club = cc.id_club and pc.season = cc.season
            where {f"id_championship = '{id_chp}'" if id_chp != "all" else "true"}
            and {f"pc.season = '{season}'" if season != "all" else "true"}
            group by p.name
            ;"""
        )
        
        cursor_teams = self.db.execute_query_get_cursor(
            f"""select c.complete_name as "Club"
            from club_championship cc
            join club c
            on cc.id_club = c.id
            where {f"id_championship = '{id_chp}'" if id_chp != "all" else "true"}
            and {f"season = '{season}'" if season != "all" else "true"}
            group by c.complete_name
            ;"""
        )
        
        if cursor_players and cursor_teams:
                players = [row[0] for row in cursor_players.fetchall()]
                cursor_players.close()
                
                teams = [row[0] for row in cursor_teams.fetchall()]
                cursor_teams.close()
                
                df = pd.DataFrame(index=players, columns=teams)
                
                for player in players:
                    cursor_oppositions = self.db.execute_query_get_cursor(
                        f"""select "Player", "Opponent", "{stat}" 
                        from players_oppositions(
                            player := '{player.replace("'", "''")}',
                            id_chp := '{id_chp}',
                            id_season := '{season}'
                            );""")
                    
                    data = cursor_oppositions.fetchall()

                    cursor_oppositions.close()
                    
                    for stats in data:
                        df.loc[stats[0], stats[1]] = stats[2]
                        
                return df
                
        return pd.DataFrame()