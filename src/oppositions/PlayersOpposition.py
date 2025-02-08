import pandas as pd

from postgres.PostgresQuerying import PostgresQuerying
#from src.oppositions.Oppositions import Oppositions

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe

    def build_oppositions(
        self,
        player: str,
        id_comp: str = 'all',
        season: str = 'all'
    ) -> pd.DataFrame:

        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")

        return self.db.df_from_query(
            f"""select * 
            from players_oppositions(
                player := '{player.replace("'", "''")}',
                id_comp := '{id_comp}',
                id_season := '{season.replace('-', '_')}'
                );""")

    def build_matrix(
        self,
        stat: str,
        id_comp: str = 'all',
        season: str = 'all'
    ) -> pd.DataFrame:

        season_schema = f"dwh_{season.replace('-', '_')}"

        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")

        cursor_players = self.db.execute_query_get_cursor(
            f"""select p.name as "Player"
            from dwh_upper.player p
            join {season_schema}.team_player tp
            on tp.player = p.id
            join {season_schema}.team t
            on tp.team = t.id
            where t.competition = '{id_comp}'
            group by p.name
            ;"""
        )

        cursor_teams = self.db.execute_query_get_cursor(
            f"""select c.name as "Club"
            from {season_schema}.team t
            join dwh_upper.club c
            on t.id = t.competition || '_' || c.id
            where t.competition = '{id_comp}'
            group by c.name
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
                        id_comp := '{id_comp}',
                        id_season := '{season.replace('-', '_')}'
                        );"""
                )

                data = cursor_oppositions.fetchall()

                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
