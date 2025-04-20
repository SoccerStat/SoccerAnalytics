import pandas as pd

from psycopg2 import sql

from postgres.postgres_querying import PostgresQuerying

class TeamsOpposition:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.ranking_sql_path = "sql/oppositions/team_x_teams"
        self.utils_sql_path = "sql/utils"

        self.db.execute_sql_file(f"{self.utils_sql_path}/schemas.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/types.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/checks.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/aggregations.sql")

    def build_oppositions(
        self,
        team: str,
        comps: list[str],
        seasons: list[str],
        # first_date: str = '1970-01-01',
        # last_date: str = '2099-12-31',
        side: str = 'both'
    ) -> pd.DataFrame:
        """
        """
        seasons_to_analyse = [season.replace('-', '_') for season in seasons]

        self.db.execute_sql_file(f"{self.ranking_sql_path}/team_x_teams.sql")

        self.db.execute_query(
            f"""
            SELECT analytics.check_side('{side}');
            """
        )

        players_ranking_tmp_table = self.db.read_sql_file(f"{self.ranking_sql_path}/tmp_tables.sql")
        self.db.execute_query(players_ranking_tmp_table)

        teams_opposition_template = self.db.read_sql_file(
            f"{self.ranking_sql_path}/template_raw_data_by_season.sql"
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
                            teams_opposition_template.format(
                                season=season,
                                id_comp=comp
                            )
                        )

        self.db.execute_sql_file(f"{self.ranking_sql_path}/team_x_teams.sql")

        query = sql.SQL("""
            select * 
            from analytics.teams_oppositions(
                team := %s,
                side := %s
                ) as "to"
            where "to"."Matches" != 0;
        """)

        return self.db.df_from_query(query, (team, side))

    def build_matrix(
        self,
        stat: str,
        id_comp: str,
        season: str,
        side: str
    ) -> list:
        """
        """

        season = season.replace('-', '_')

        season_schema = f"season_{season}"

        self.db.execute_sql_file(f"{self.ranking_sql_path}/team_x_teams.sql")

        query = sql.SQL("""
            select c.name as "Club"
                from {}.team t
                join upper.club c
                on t.club = c.id
                where t.competition = %s
                group by c.name;
        """).format(sql.Identifier(season_schema))

        cursor_instance = self.db.execute_query_get_cursor(query, (id_comp,))

        if cursor_instance:
            teams = [row[0] for row in cursor_instance.fetchall()]
            cursor_instance.close()
            df = pd.DataFrame(index=teams, columns=teams)

            for team in teams:
                team_query = sql.SQL("""
                    select "Team", "Opponent", {stat}
                    from analytics.teams_oppositions(
                        team := %s,
                        side := %s
                    );
                """).format(stat=sql.Identifier(stat))

                cursor_oppositions = self.db.execute_query_get_cursor(team_query,
                    (team, side)
                )

                data = cursor_oppositions.fetchall()
                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
