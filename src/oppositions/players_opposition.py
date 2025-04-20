import pandas as pd
from psycopg2 import sql

from src.postgres.postgres_querying import PostgresQuerying
#from src.oppositions.Oppositions import Oppositions

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.opposition_sql_path = "sql/oppositions/player_x_teams"
        self.utils_sql_path = "sql/utils"

        self.db.execute_sql_file(f"{self.utils_sql_path}/schemas.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/types.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/checks.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/aggregations.sql")

    def build_oppositions(
        self,
        player: str,
        comps: list[str],
        seasons: list[str],
        side: str = "both"
    ) -> pd.DataFrame:
        """
        """
        seasons_to_analyse = [season.replace('-', '_') for season in seasons]

        self.db.execute_query(
            f"""
            SELECT analytics.check_side('{side}');
            """
        )

        players_opposition_tmp_table = self.db.read_sql_file(f"{self.opposition_sql_path}/tmp_tables.sql")
        self.db.execute_query(players_opposition_tmp_table)

        players_opposition_template = self.db.read_sql_file(
            f"{self.opposition_sql_path}/template_raw_data_by_season.sql"
        )

        all_season_schemas_query = sql.SQL("""select * from analytics.get_season_schemas();""")
        all_season_schemas = self.db.df_from_query(all_season_schemas_query).iloc[:, 0].tolist()

        all_comps_query = sql.SQL("""select * from analytics.get_competitions();""")
        all_comps = self.db.df_from_query(all_comps_query).iloc[:, 0].tolist()

        for season_schema in all_season_schemas:
            season = season_schema[7:]
            if seasons_to_analyse == [] or season in seasons_to_analyse:
                for comp in all_comps:
                    if comps == [] or comp in comps:
                        self.db.execute_query(
                            f"""
                            SELECT analytics.check_id_comp('{comp}');
                            """
                        )

                        self.db.execute_query(
                            players_opposition_template.format(
                                season=season,
                                id_comp=comp
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
