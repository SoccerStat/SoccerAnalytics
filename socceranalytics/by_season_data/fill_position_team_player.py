from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader

from socceranalytics.utils.logging import log


class TeamPlayer:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.data_loader = DataLoader(postgres_to_dataframe)
        self.sql_path = "socceranalytics.sql.by_season_data"

    def update_team_player_table(self):
        log("\tUpdating the 'Team Player' table of each season...")
        by_season_query = self.db.read_sql_file(self.sql_path, "get_most_played_positions.sql")

        for season in self.data_loader.get_seasons():
            log(f"\t{season}")
            self.db.execute_query(
                by_season_query.format(
                    season=season,
                )
            )

        log("\tPositions updated for each season...")
