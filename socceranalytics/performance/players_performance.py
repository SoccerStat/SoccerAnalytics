from socceranalytics.performance.base_performance import BasePerformance
from socceranalytics.postgres.postgres_querying import PostgresQuerying

from socceranalytics.utils.logging import log


class PlayersPerformance(BasePerformance):
    """Fill teams performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        super().__init__(postgres_to_dataframe, "players")

    def process_performance_table(self):
        """Truncate and fill the staging_players_performance
        tables in the analytics schema.
        Supposed to be ran once a day.
        Used to build rankings and opposition tables.
        """
        log("Truncating the Players' performance table...")
        self.db.execute_sql_file(self.performance_sql_path, "truncate_performance_table.sql")

        log("Filling the Players' performance table...")
        teams_ranking_template = self.db.read_sql_file(self.performance_sql_path, "fill_performance_table.sql")

        for season in self.data_loader.get_seasons():
            log(f"\t{season}")
            for id_comp in self.data_loader.get_competition_ids():
                log(f"\t\t{id_comp}")
                self.db.execute_query(
                    teams_ranking_template.format(
                        season=season,
                        id_comp=id_comp,
                    )
                )

        n_rows_inserted_perf_table = self.db.execute_query(
            "SELECT count(*) from analytics.staging_players_performance;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[PERFORMANCE TABLE] Rows inserted: {n_rows_inserted_perf_table}")
