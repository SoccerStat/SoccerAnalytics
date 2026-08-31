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

    def process_points_deductions_table(self):
        """Get all teams we may have that have participated in any competition at any season.
        """
        log("\tTruncating the Teams' points deductions table...")
        self.db.execute_sql_file(self.sql_path, "truncate_points_deductions_table.sql")

        log("\tFilling the Teams' points deductions table...")
        teams_ranking_template = self.db.read_sql_file(self.sql_path, "fill_points_deductions_table.sql")

        for season in self.data_loader.get_seasons():
            log(f"\t\t{season}")
            for id_comp in self.data_loader.get_competition_ids():
                self.db.execute_query(
                    teams_ranking_template.format(
                        id_comp=id_comp,
                        season=season,
                    )
                )

        n_rows_inserted_table = self.db.execute_query(
            "SELECT count(*) from analytics.staging_teams_points_deductions;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[POINTS DEDUCTIONS TABLE] Rows inserted: {n_rows_inserted_table}")
