import numpy as np
import pandas as pd

from postgres.PostgresQuerying import PostgresQuerying

class TeamsRanking:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe

    def __simulate_matches(
        self,
        teams: pd.DataFrame,
        n_sim: int,
        r: int) -> pd.DataFrame:

        def xG_per_shot(shots: pd.Series, xg: pd.Series, n: int):
            return np.random.binomial(shots, xg/shots, size=(n,))

        def simulate_by_side(played_home: bool):
            team = teams.loc[teams['played_home'] == played_home, ['shots', 'xg']]
            return xG_per_shot(team['shots'], team['xg'], n_sim)

        home_xgps = simulate_by_side(True)
        away_xgps = simulate_by_side(False)

        home_xp = np.where(
            home_xgps > away_xgps,
            3,
            np.where(home_xgps == away_xgps, 1, 0))
        away_xp = np.where(home_xp == 1, 1, 3-home_xp)

        # Average number of points per simulation
        def sum_and_round(xp):
            return round(np.mean(xp), r)

        teams.loc[teams['played_home'] == True, 'xP'] = sum_and_round(home_xp)
        teams.loc[teams['played_home'] == False, 'xP'] = sum_and_round(away_xp)

        return teams[['played_home', 'Club', 'xP']]


    def __build_justice_ranking(
        self,
        team_stats: pd.DataFrame,
        side: str,
        n_sim: int,
        r: int) -> pd.DataFrame:

        if side == 'home':
            side_filter = 'played_home'
        elif side == 'away':
            side_filter = 'not played_home'
        #elif side == 'both':
        side_filter = 'played_home | not played_home'

        if not team_stats.empty:
            return \
                pd.concat(
                    [self.__simulate_matches(
                        team_stats.loc[team_stats['match'] == id_match].copy(),
                        n_sim,
                        r
                    ) for id_match in team_stats['match'].unique()]
                ) \
                    .query(side_filter)[['Club', 'xP']] \
                        .groupby('Club') \
                            .sum('xP')

        return pd.DataFrame(columns=['Club', 'xP'])

    def __merge_rankings(
        self,
        merged_ranking: pd.DataFrame,
        justice_ranking: pd.DataFrame,
        r: int
        ) -> pd.DataFrame:

        merged_ranking = pd.merge(
            merged_ranking,
            justice_ranking,
            left_on='Club',
            right_on='Club')

        merged_ranking['xP/Match'] = \
            round(merged_ranking['xP'] / merged_ranking['Matches'], r)

        merged_ranking['Diff Points'] = \
            merged_ranking['Points'] - merged_ranking['xP']

        return merged_ranking.set_index("Ranking")

    def build_ranking(
        self,
        id_comp: str,
        season: str,
        first_week: int = 1,
        last_week: int = 100,
        side: str = 'both',
        n_sim: int = 1000000,
        r: int = 2) -> pd.DataFrame:

        season = season.replace('-', '_')

        self.db.execute_sql_file("sql/rankings/sub_functions.sql")
        self.db.execute_sql_file("sql/rankings/teams.sql")
        self.db.execute_sql_file("sql/rankings/teams_xp.sql")

        teams_ranking = self.db.df_from_query( \
            f"""
            select *
            from dwh_utils.teams_ranking(
                id_comp := '{id_comp}',
                id_season := '{season}',
                first_week := {first_week},
                last_week := {last_week},
                side := '{side}'
                );""")

        justice_ranking = self.__build_justice_ranking( \
            self.db.df_from_query(
                f"""
                select * 
                from dwh_utils.teams_justice_table(
                    id_comp := '{id_comp}', 
                    id_season := '{season}', 
                    first_week := {first_week},
                    last_week := {last_week}
                    );"""),
            side,
            n_sim,
            r
        )

        try:
            return self.__merge_rankings(teams_ranking, justice_ranking, r)
        except:
            print("Erreur while merging dataframes.")
            