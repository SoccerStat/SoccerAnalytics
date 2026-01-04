from datetime import datetime
import pandas as pd
from psycopg2 import sql

from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader


class PlayersRanking:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.ranking_sql_path = "socceranalytics.sql.rankings.players"
        self.data_loader = DataLoader(postgres_to_dataframe)
        self.db.execute_sql_file(self.ranking_sql_path, "players.sql")

    def build_ranking(
        self,
        seasons: list[str],
        comps: list[str],
        first_week: int = 1,
        last_week: int = 100,
        first_date: str = '1970-01-01',
        last_date: str = '2099-12-31',
        side: str = 'both',
        r: int = 2
    ) -> pd.DataFrame:
        """Build the players ranking
        """

        seasons = [season.replace('-', '_') for season in seasons]
        seasons = seasons if seasons else self.data_loader.get_seasons(min_season)
        comps = comps if comps else self.data_loader.get_competition_names()

        self.db.execute_query(
            f"""
            SELECT analytics.check_weeks('{first_week}', '{last_week}');
            SELECT analytics.check_dates('{first_date}', '{last_date}');
            SELECT analytics.check_side('{side}');
            """
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

        query = sql.SQL("""
            select *
            from analytics.players_rankings(
                seasons := %s,
                comps := %s,
                first_week := %s,
                last_week := %s,
                first_date := %s,
                last_date := %s,
                side := %s,
                r := %s
            );
        """)  # .set_index('Ranking')

        return self.db.df_from_query(
            query,
            (seasons, comps, first_week, last_week, first_date, last_date, side, r)
        )

    def build_ranking_wrapper(
        self,
        seasons: tuple[str],
        comps: tuple[str],
        first_week: int,
        last_week: int,
        first_date: datetime,
        last_date: datetime,
        side: str
    ):
        return self.build_ranking(
            seasons=list(seasons),
            comps=list(comps),
            first_week=first_week,
            last_week=last_week,
            first_date=first_date.strftime('%Y-%m-%d'),
            last_date=last_date.strftime('%Y-%m-%d'),
            side=side,
            r=2
        )
