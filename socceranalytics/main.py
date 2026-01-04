from socceranalytics.all_items.all_teams import AllTeams
from socceranalytics.by_season_data.fill_position_team_player import TeamPlayer
from socceranalytics.performance.teams_performance import TeamsPerformance
from socceranalytics.performance.players_performance import PlayersPerformance

from socceranalytics.postgres.postgres_querying import PostgresQuerying

from socceranalytics.data.paths import Config

from socceranalytics.utils.utils import get_ti, parse_args
from socceranalytics.logging import log


def run(args):
    env = args.env
    min_season = args.min_season

    start_time = get_ti()

    Config.load_env_files(args.env)
    log(f"----- {env} environment -----")

    db = PostgresQuerying()

    teams_performance = TeamsPerformance(db)
    teams_performance.process_performance_table(min_season)
    teams_performance.process_mapping_clubs_table()

    all_teams = AllTeams(db)
    all_teams.process_all_teams_table(min_season)

    players_performance = PlayersPerformance(db)
    players_performance.process_performance_table(min_season)

    team_player = TeamPlayer(db)
    team_player.update_team_player_table(min_season)

    end_time = get_ti()
    log(f"--- {end_time - start_time} ---")
    log("Finished!")


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
