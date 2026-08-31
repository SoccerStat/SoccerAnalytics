from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader

from socceranalytics.utils.logging import log


class PointsDeductions:
    """Fill teams performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.data_loader = DataLoader(postgres_to_dataframe)
        self.sql_path = "socceranalytics.sql.misc.points_deductions"

    def process_points_deductions_table(self, min_season, max_season):
        """Get all teams we may have that have participated in any competition at any season.
        """
        log("\tTruncating the Teams' points deductions table...")
        self.db.execute_sql_file(self.sql_path, "truncate_points_deductions_table.sql")

        log("\tFilling the Teams' points deductions table...")
        points_deductions_template = self.db.read_sql_file(self.sql_path, "fill_points_deductions_table.sql")

        seasons = self.data_loader.get_seasons(min_season, max_season)

        for season in seasons:
            log(f"\t\t{season}")
            self.db.execute_query(points_deductions_template.format(season=season))

        n_rows_inserted_table = self.db.execute_query(
            "SELECT count(*) from analytics.staging_teams_points_deductions;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[POINTS DEDUCTIONS TABLE] Rows inserted: {n_rows_inserted_table}")
