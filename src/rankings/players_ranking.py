import pandas as pd
from psycopg2 import sql

from postgres.postgres_querying import PostgresQuerying

class PlayersRanking:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.ranking_sql_path = "sql/rankings/players"
        self.utils_sql_path = "sql/utils"

        self.db.execute_sql_file(f"{self.utils_sql_path}/schemas.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/types.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/checks.sql")
        # self.db.execute_sql_file(f"{self.utils_sql_path}/selected_matches.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/aggregations.sql")

    def build_ranking(
        self,
        comps: list[str],
        seasons: list[str],
        first_week: int = 1,
        last_week: int = 100,
        first_date: str = '1970-01-01',
        last_date: str = '2099-12-31',
        side: str = 'both',
        r: int = 2
    ) -> pd.DataFrame:
        """
        """
        seasons_to_analyse = [season.replace('-', '_') for season in seasons]

        self.db.execute_query(
            f"""
            SELECT analytics.check_weeks('{first_week}', '{last_week}');
            SELECT analytics.check_dates('{first_date}', '{last_date}');
            SELECT analytics.check_side('{side}');
            """
        )

        players_ranking_tmp_table = self.db.read_sql_file(f"{self.ranking_sql_path}/tmp_tables.sql")
        self.db.execute_query(players_ranking_tmp_table)

        players_ranking_template = self.db.read_sql_file(
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
                            players_ranking_template.format(
                                season=season,
                                id_comp=comp,
                                first_week=first_week,
                                last_week=last_week,
                                first_date=first_date,
                                last_date=last_date
                            )
                        )

        self.db.execute_sql_file(f"{self.ranking_sql_path}/players.sql")

        query = sql.SQL("""
            select * 
            from analytics.players_rankings(
                side := %s,
                r := %s
            );
        """) #.set_index('Ranking')

        return self.db.df_from_query(query, (side, r))
