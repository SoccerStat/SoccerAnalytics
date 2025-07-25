from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader

from socceranalytics.utils.logging import log

class AllTeams:
    """Fill teams performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.data_loader = DataLoader(postgres_to_dataframe)
        self.sql_path = f"socceranalytics.sql.all_items.teams"

    def process_all_teams_table(self):
        """Get all teams we may have that have participated in any competition at any season.
        """
        log("\tTruncating the Teams' table...")
        self.db.execute_sql_file(self.sql_path, "truncate_all_teams_table.sql")

        log("\tFilling the Teams' table...")
        teams_ranking_template = self.db.read_sql_file(self.sql_path, "fill_all_teams_table.sql")

        for season in self.data_loader.get_seasons():
            for id_comp in self.data_loader.get_competition_ids():
                self.db.execute_query(
                    teams_ranking_template.format(
                        id_comp=id_comp,
                        season=season,
                    )
                )

        n_rows_inserted_table = self.db.execute_query(
            "SELECT count(*) from analytics.all_teams;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[ALL TEAMS TABLE] Rows inserted: {n_rows_inserted_table}")
