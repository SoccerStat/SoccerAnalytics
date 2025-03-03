import numpy as np
import pandas as pd
from psycopg2 import sql

from postgres.PostgresQuerying import PostgresQuerying

class TeamsRanking:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.ranking_sql_path = "sql/rankings/teams"
        self.utils_sql_path = "sql/utils"

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
        teams_ranking: pd.DataFrame,
        justice_ranking: pd.DataFrame,
        r: int = 2
        ) -> pd.DataFrame:

        merged_ranking = pd.merge(
            teams_ranking,
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
        seasons: list,
        first_week: int = 1,
        last_week: int = 100,
        side: str = 'both',
        n_sim: int = 1000000,
        r: int = 2,
        justice_ranking: bool = True
    ) -> pd.DataFrame:

        self.db.execute_sql_file(f"{self.utils_sql_path}/schemas.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/types.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/checks.sql")
        self.db.execute_sql_file(f"{self.utils_sql_path}/aggregations.sql")

        teams_ranking_tmp_table = self.db.read_sql_file(f"{self.utils_sql_path}/tmp_tables.sql")
        self.db.execute_query(teams_ranking_tmp_table)

        teams_ranking_template = self.db.read_sql_file(
            f"{self.ranking_sql_path}/template_raw_data_by_season.sql"
        )

        if justice_ranking:
            justice_ranking_template = self.db.read_sql_file(
                f"{self.ranking_sql_path}/template_xp_by_season.sql"
            )

        all_season_schemas_query = sql.SQL("""select * from dwh_utils.get_season_schemas();""")
        all_season_schemas = self.db.df_from_query(all_season_schemas_query).iloc[:, 0].tolist()

        for season_schema in all_season_schemas:
            season = season_schema[4:]
            if seasons == [] or season in seasons:
                self.db.execute_query(
                    teams_ranking_template.format(
                        season=season,
                        id_comp=id_comp,
                        first_week=first_week,
                        last_week=last_week
                    )
                )

                if justice_ranking:
                    self.db.execute_query(
                        justice_ranking_template.format(
                            season=season,
                            id_comp=id_comp,
                            first_week=first_week,
                            last_week=last_week
                        )
                    )

        self.db.execute_sql_file(f"{self.ranking_sql_path}/teams.sql")
        seasons_teams_ranking_query = sql.SQL("""
            select *
            from dwh_utils.teams_ranking(
                side := %s,
                r := %s
                );
        """)

        teams_ranking = self.db.df_from_query(seasons_teams_ranking_query, (side, r))

        if justice_ranking:
            seasons_justice_ranking_query = sql.SQL("""
                select *
                from tmp_justice_ranking;
            """)

            justice_ranking = self.__build_justice_ranking(
                self.db.df_from_query(seasons_justice_ranking_query),
                side,
                n_sim,
                r
            )

            return self.__merge_rankings(teams_ranking, justice_ranking, r)            

        return teams_ranking

    # def build_ranking_by_season(
    #     self,
    #     id_comp: str,
    #     season_schema: str,
    #     first_week: int = 1,
    #     last_week: int = 100,
    #     side: str = 'both',
    #     n_sim: int = 1000000,
    #     r: int = 2
    # ) -> pd.DataFrame:

    #     self.db.execute_sql_file("sql/utils/schemas.sql")
    #     self.db.execute_sql_file("sql/utils/types.sql")
    #     self.db.execute_sql_file("sql/utils/checks.sql")
    #     self.db.execute_sql_file("sql/utils/aggregations.sql")
    #     self.db.execute_sql_file("sql/rankings/teams/teams_by_season.sql")

    #     teams_query = sql.SQL("""
    #         select *
    #         from dwh_utils.teams_ranking_by_season(
    #             id_comp := %s,
    #             season_shema := %s,
    #             first_week := %s,
    #             last_week := %s,
    #             side := %s,
    #             r := %s
    #             );
    #     """)

    #     teams_ranking = self.db.df_from_query(
    #         teams_query,
    #         (id_comp, season_schema, first_week, last_week, side, r)
    #     )

    #     # justice_query = sql.SQL("""
    #     #     select *
    #     #     from dwh_utils.teams_justice_table(
    #     #         id_comp := %s,
    #     #         season_schema := %s,
    #     #         first_week := %s,
    #     #         last_week := %s
    #     #         );
    #     # """)

    #     # justice_ranking = self.__build_justice_ranking(
    #     #     self.db.df_from_query(justice_query, (id_comp, season_schema, first_week, last_week)),
    #     #     side,
    #     #     n_sim,
    #     #     r
    #     # )
    #     justice_ranking = pd.DataFrame()

    #     if not justice_ranking.empty:
    #         return self.__merge_rankings(teams_ranking, justice_ranking, r)

    #     return teams_ranking
