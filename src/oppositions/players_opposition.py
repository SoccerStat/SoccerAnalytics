import pandas as pd
from psycopg2 import sql

from postgres.postgres_querying import PostgresQuerying
#from src.oppositions.Oppositions import Oppositions

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe

    def build_oppositions(
        self,
        player: str,
        id_comp: str,
        season: str,
        side: str = "both"
    ) -> pd.DataFrame:

        self.db.execute_sql_file("sql/rankings/sub_functions.sql")
        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")

        query = sql.SQL("""
            select * 
            from analytics.players_oppositions(%s, %s, %s, %s) as "po"
            where "po"."Matches" != 0;
        """)

        return self.db.df_from_query(query, (player.replace("'", "''"), id_comp, season.replace('-', '_'), side))

    def build_matrix(
        self,
        stat: str,
        id_comp: str,
        season: str,
        side: str
    ) -> pd.DataFrame:

        season = season.replace('-', '_')

        season_schema = f"season_{season}"

        self.db.execute_sql_file("sql/oppositions/player_x_teams.sql")

        query = sql.SQL("""
            select p.name as "Player"
            from upper.player p
            join {}.team_player tp
            on tp.player = p.id
            join {}.team t
            on tp.team = t.id
            where t.competition = %s
            group by p.name;
        """).format(sql.Identifier(season_schema), sql.Identifier(season_schema))

        cursor_players = self.db.execute_query_get_cursor(query, (id_comp,))

        teams_query = sql.SQL("""
            select c.name as "Club"
            from {}.team t
            join upper.club c
            on t.id = t.competition || '_' || c.id
            where t.competition = %s
            group by c.name;
        """).format(sql.Identifier(season_schema))

        cursor_teams = self.db.execute_query_get_cursor(teams_query, (id_comp,))

        if cursor_players and cursor_teams:
            players = [row[0] for row in cursor_players.fetchall()]
            cursor_players.close()

            teams = [row[0] for row in cursor_teams.fetchall()]
            cursor_teams.close()

            df = pd.DataFrame(index=players, columns=teams)

            for player in players:
                player_query = sql.SQL("""
                    select "Player", "Opponent", {}
                    from analytics.players_oppositions(%s, %s, %s, %s);
                """).format(sql.Identifier(stat))

                cursor_oppositions = self.db.execute_query_get_cursor(player_query,
                    (player.replace("'", "''"), id_comp, season, side)
                )

                data = cursor_oppositions.fetchall()

                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
