from psycopg2 import sql

from socceranalytics.performance.base_performance import BasePerformance
from socceranalytics.performance.understat__get_xG import get_teams_xG
from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.comp_helper import CompHelper

from socceranalytics.utils.logging import log

class TeamsPerformance(BasePerformance, CompHelper):
    """Fill teams performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        super().__init__(postgres_to_dataframe, "teams")
        CompHelper.__init__(self)

    def process_performance_table(self):
        """Truncate and fill the staging_teams_performance and staging_teams_expected_performance
        tables in the analytics schema.
        Supposed to be run once a day.
        Used to build rankings and opposition tables.
        """
        log("\tTruncating the Teams' performance table...")
        self.db.execute_sql_file(self.performance_sql_path, "truncate_performance_tables.sql")

        log("\tFilling the Teams' performance table...")
        teams_ranking_template = self.db.read_sql_file(self.performance_sql_path, "fill_performance_table.sql")

        log("\tFilling the Teams' expected performance table...")
        insert_query = sql.SQL("""
            INSERT INTO understat.staging_teams_understat_performance
            ("match", "competition", "season", "name_team", "name_opponent", "played_home", "home_xg_for", "away_xg_for", "home_xg_against", "away_xg_against")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("match", "played_home") DO NOTHING
        """)
        # expected_performance_ranking_template = self.db.read_sql_file(self.performance_sql_path, "fill_expected_performance_table.sql")

        for season in self.data_loader.get_seasons():
            for id_comp,name_comp in zip(self.data_loader.get_competition_ids(), self.data_loader.get_competition_names()):
                self.db.execute_query(
                    teams_ranking_template.format(
                        season=season,
                        id_comp=id_comp,
                    )
                )

                # self.db.execute_query(
                #     expected_performance_ranking_template.format(
                #         season=season,
                #         id_comp=id_comp
                #     )
                # )

                if ("UEFA" not in name_comp) and (season >= "2014_2015"):
                    understat_comp = super().get_understat_comp_from_soccerstat(name_comp)
                    xG_by_match = get_teams_xG(understat_comp, season[:4])
                    for match in xG_by_match:
                        self.db.execute_query(
                            insert_query,
                            (
                                match["match"],
                                name_comp,
                                season,
                                match["name_team"],
                                match["name_opponent"],
                                match["played_home"],
                                match["home_xg_for"],
                                match["away_xg_for"],
                                match["home_xg_against"],
                                match["away_xg_against"]
                            )
                        )

        n_rows_inserted_perf_table = self.db.execute_query(
            "SELECT count(*) from analytics.staging_teams_performance;",
            return_cursor=True
        ).fetchone()[0]

        # n_rows_inserted_exp_perf_table = self.db.execute_query(
        #     "SELECT count(*) from analytics.staging_teams_expected_performance;",
        #     return_cursor=True
        # ).fetchone()[0]

        n_rows_inserted_exp_perf_understat_table = self.db.execute_query(
            "SELECT count(*) from understat.staging_teams_understat_performance;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[PERFORMANCE TABLE] Rows inserted: {n_rows_inserted_perf_table}")
        # log(f"[EXPECTED PERFORMANCE TABLE] Rows inserted: {n_rows_inserted_exp_perf_table}")
        log(f"[EXPECTED PERFORMANCE UNDERSTAT TABLE] Rows inserted: {n_rows_inserted_exp_perf_understat_table}")

    def process_mapping_clubs_table(self):
        log("\tTruncating the mapping clubs table...")
        self.db.execute_sql_file(self.performance_sql_path, "truncate_mapping_clubs_table.sql")

        log("\tFilling the mapping clubs table...")
        self.db.execute_sql_file(self.performance_sql_path, "fill_mapping_clubs_table.sql")

        n_rows_mapping_clubs_table = self.db.execute_query(
            "SELECT count(*) from analytics.mapping_clubs_soccerstat_understat;",
            return_cursor=True
        ).fetchone()[0]

        log(f"[MAPPING CLUBS TABLE] Rows inserted: {n_rows_mapping_clubs_table}")