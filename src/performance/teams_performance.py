from src.performance.base_performance import BasePerformance
from src.postgres.postgres_querying import PostgresQuerying

class TeamsPerformance(BasePerformance):
    """Fill teams performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        super().__init__(postgres_to_dataframe, "teams")

    def process_performance_table(self):
        """Truncate and fill the staging_teams_performance and staging_teams_expected_performance
        tables in the analytics schema.
        Supposed to be ran once a day.
        Used to build rankings and opposition tables.
        """
        self.db.execute_sql_file(f"{self.performance_sql_path}/truncate_performance_tables.sql")

        teams_ranking_template = self.db.read_sql_file(
            f"{self.ranking_sql_path}/template_raw_data_by_season.sql"
        )

        justice_ranking_template = self.db.read_sql_file(
            f"{self.ranking_sql_path}/template_xp_by_season.sql"
        )

        # teams_opposition_template = self.db.read_sql_file(
        #     f"{self.opposition_sql_path}/template_raw_data_by_season.sql"
        # )

        for season in self.data_loader.get_seasons():
            for id_comp in self.data_loader.get_competition_ids():
                self.db.execute_query(
                    teams_ranking_template.format(
                        season=season,
                        id_comp=id_comp,
                    )
                )

                self.db.execute_query(
                    justice_ranking_template.format(
                        season=season,
                        id_comp=id_comp
                    )
                )

                # self.db.execute_query(
                #     teams_opposition_template.format(
                #         season=season,
                #         comp=comp
                #     )
                # )
