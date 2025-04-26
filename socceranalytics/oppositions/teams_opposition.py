import pandas as pd

from psycopg2 import sql

from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader


class TeamsOpposition:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.opposition_sql_path = "socceranalytics.oppositions.teams_x_teams"
        self.data_loader = DataLoader(postgres_to_dataframe)

        self.db.execute_sql_file(self.opposition_sql_path, "team_x_teams.sql")

    def build_oppositions(
        self,
        team: str,
        seasons: list[str],
        comps: list[str],
        side: str = 'both'
    ) -> pd.DataFrame:
        """Build Oppositions table between a teams and teams it played against.
        """

        seasons = [season.replace('-', '_') for season in seasons]
        seasons = seasons if seasons else self.data_loader.get_seasons()
        comps = comps if comps else self.data_loader.get_competition_names()

        self.db.execute_query(
            f"""
            SELECT analytics.check_side('{side}');
            """
        )

        for season in seasons:
            self.db.execute_query(
                f"""
                SELECT analytics.check_season('{season}');
                """
            )

        for comp in comps:
            self.db.execute_query(
                f"""
                SELECT analytics.check_comp('{comp}');
                """
            )

        query = sql.SQL("""
            select *
            from analytics.teams_oppositions(
                seasons := %s,
                comps := %s,
                team := %s,
                side := %s
            ) as "to"
            where "to"."Matches" != 0;
        """)

        return self.db.df_from_query(
            query,
            (seasons, comps, team.replace("'", "''"), side)
        )

    def build_oppositions_wrapper(
        self,
        team: str,
        seasons: tuple[str],
        comps: tuple[str],
        side: str
    ):
        return self.build_oppositions(
            team=team,
            comps=list(comps),
            seasons=list(seasons),
            side=side
        )

    def build_matrix(
        self,
        stat: str,
        seasons: list[str],
        comps: list[str],
        side: str
    ) -> pd.DataFrame:
        """Build team x team matrix
        """

        seasons = [season.replace('-', '_') for season in seasons]
        seasons = seasons if seasons else self.data_loader.get_seasons()
        comps = comps if comps else self.data_loader.get_competition_names()

        self.db.execute_query(
            f"""
            SELECT analytics.check_side('{side}');
            """
        )

        for season in seasons:
            self.db.execute_query(
                f"""
                SELECT analytics.check_season('{season}');
                """
            )

        for comp in comps:
            self.db.execute_query(
                f"""
                SELECT analytics.check_comp('{comp}');
                """
            )

        all_teams = []
        for season in seasons:
            for comp in comps:
                query = sql.SQL("""
                    select c.name as "Club"
                        from {}.team t
                        join upper.club c
                        on t.club = c.id
                        left join upper.championship chp
                        on t.competition = chp.id
                        left join upper.continental_cup c_cup
                        on t.competition = c_cup.id
                        where %s in (chp.name, c_cup.name)
                        group by c.name;
                """).format(sql.Identifier(f"season_{season}"))

                cursor_instance = self.db.execute_query(query, (comp,), return_cursor=True)
                if cursor_instance:
                    all_teams += [row[0] for row in cursor_instance.fetchall() if row[0] not in all_teams]
                    cursor_instance.close()

        df = pd.DataFrame(index=all_teams, columns=all_teams)

        for team in all_teams:
            team_query = sql.SQL("""
                select "Team", "Opponent", {stat}
                from analytics.teams_oppositions(
                    seasons := %s,
                    comps := %s,
                    team := %s,
                    side := %s
                );
            """).format(stat=sql.Identifier(stat))

            cursor_oppositions = self.db.execute_query(
                team_query,
                (seasons, comps, team, side),
                return_cursor=True
            )

            data = cursor_oppositions.fetchall()
            cursor_oppositions.close()

            for stats in data:
                df.loc[stats[0], stats[1]] = stats[2]

        return df

    def build_matrix_wrapper(
        self,
        stat: str,
        seasons: tuple[str],
        comps: tuple[str],
        side: str
    ):
        return self.build_matrix(
            stat=stat,
            seasons=list(seasons),
            comps=list(comps),
            side=side
        )
