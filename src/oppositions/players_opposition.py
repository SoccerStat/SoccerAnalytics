import pandas as pd
from psycopg2 import sql

from src.postgres.postgres_querying import PostgresQuerying
from src.utils.data_loader import DataLoader

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.opposition_sql_path = "sql/oppositions/player_x_teams"
        self.data_loader = DataLoader(postgres_to_dataframe)

    def build_oppositions(
        self,
        player: str,
        comps: list[str],
        seasons: list[str],
        side: str = "both"
    ) -> pd.DataFrame:
        """Build Oppositions table between a player and teams he played against.
        """

        seasons = [season.replace('-', '_') for season in seasons]

        self.db.execute_query(
            f"""
            SELECT analytics.check_side('{side}');
            """
        )

        players_opposition_tmp_table = self.db.read_sql_file(
            f"{self.opposition_sql_path}/tmp_tables.sql"
        )
        self.db.execute_query(players_opposition_tmp_table)

        players_opposition_template = self.db.read_sql_file(
            f"{self.opposition_sql_path}/template_raw_data_by_season.sql"
        )

        for season in seasons:
            self.db.execute_query(
                f"""
                SELECT analytics.check_season('{season}');
                """
            )
            for comp in comps:
                self.db.execute_query(
                    f"""
                    SELECT analytics.check_comp('{comp}');
                    """
                )

                self.db.execute_query(
                    players_opposition_template.format(
                        season=season,
                        comp=comp
                    )
                )

        self.db.execute_sql_file(f"{self.opposition_sql_path}/player_x_teams.sql")

        query = sql.SQL("""
            select * 
            from analytics.players_oppositions(
                player := %s,
                side := %s
            ) as "po"
            where "po"."Matches" != 0;
        """)

        return self.db.df_from_query(query, (player.replace("'", "''"), side))

    def build_matrix(
        self,
        stat: str,
        comp: str,
        season: str,
        side: str
    ) -> pd.DataFrame:

        season = season.replace('-', '_')

        season_schema = f"season_{season}"

        self.db.execute_sql_file(f"{self.opposition_sql_path}/player_x_teams.sql")

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

        cursor_players = self.db.execute_query(query, (comp,), return_cursor=True)

        teams_query = sql.SQL("""
            select c.name as "Club"
            from {}.team t
            join upper.club c
            on t.id = t.competition || '_' || c.id
            where t.competition = %s
            group by c.name;
        """).format(sql.Identifier(season_schema))

        cursor_teams = self.db.execute_query(teams_query, (comp,), return_cursor=True)

        if cursor_players and cursor_teams:
            players = [row[0] for row in cursor_players.fetchall()]
            cursor_players.close()

            teams = [row[0] for row in cursor_teams.fetchall()]
            cursor_teams.close()

            df = pd.DataFrame(index=players, columns=teams)

            for player in players:
                player_query = sql.SQL("""
                    select "Player", "Opponent", {}
                    from analytics.players_oppositions(%s, %s);
                """).format(sql.Identifier(stat))

                cursor_oppositions = self.db.execute_query(
                    player_query,
                    (player.replace("'", "''"), side),
                    return_cursor=True
                )

                data = cursor_oppositions.fetchall()

                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
