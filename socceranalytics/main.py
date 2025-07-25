import argparse

from socceranalytics.performance.teams_performance import TeamsPerformance
from socceranalytics.performance.players_performance import PlayersPerformance

from socceranalytics.postgres.postgres_querying import PostgresQuerying

from socceranalytics.data.paths import Config

from socceranalytics.utils.utils import Utils
from socceranalytics.utils.logging import log


def run(args):
    env = args.env

    try:
        utils = Utils()
        start_time = utils.get_ti()

        Config.load_env_files(args.env)
        log(f"----- {env} environment -----")

        db = PostgresQuerying()

        teams_performance = TeamsPerformance(db)
        teams_performance.process_performance_table()

        all_teams = AllTeams(db)
        all_teams.process_all_teams_table()

        players_performance = PlayersPerformance(db)
        players_performance.process_performance_table()

        end_time = utils.get_ti()
        log(f"--- {end_time - start_time} ---")
        log("Finished!")

    except Exception as e:
        log(f"An error occurred: {e}", exception=True)


def main():
    parser = argparse.ArgumentParser(description='Script de mise à jour des tables analytiques')

    parser.add_argument("--env", type=str, required=True, choices=["dev", "prod"])

    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
