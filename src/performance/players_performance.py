from src.performance.base_performance import BasePerformance
from src.postgres.postgres_querying import PostgresQuerying


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
        self.db.execute_sql_file(f"{self.performance_sql_path}/truncate_performance_tables.sql")

        teams_ranking_template = self.db.read_sql_file(
            f"{self.performance_sql_path}/fill_performance_table.sql"
        )

        for season in self.data_loader.get_seasons():
            for id_comp in self.data_loader.get_competition_ids():
                self.db.execute_query(
                    teams_ranking_template.format(
                        season=season,
                        id_comp=id_comp,
                    )
                )
