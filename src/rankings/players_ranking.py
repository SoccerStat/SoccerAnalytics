import pandas as pd
from psycopg2 import sql

from src.postgres.postgres_querying import PostgresQuerying
from src.utils.data_loader import DataLoader

class PlayersRanking:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.ranking_sql_path = "sql/rankings/players"
        self.data_loader = DataLoader(postgres_to_dataframe)

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
        """Build the players ranking
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

        all_seasons = self.data_loader.get_seasons()
        all_comps = self.data_loader.get_competition_ids()

        for season in all_seasons:
            self.db.execute_query(
                f"""
                SELECT analytics.check_season('{season}');
                """
            )
            for comp in all_comps:
                self.db.execute_query(
                    f"""
                    SELECT analytics.check_comp('{comp}');
                    """
                )

                self.db.execute_query(
                    players_ranking_template.format(
                        season=season,
                        comp=comp,
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
