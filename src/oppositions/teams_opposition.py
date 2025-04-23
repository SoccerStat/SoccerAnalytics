import pandas as pd

from psycopg2 import sql

from src.postgres.postgres_querying import PostgresQuerying
from src.utils.data_loader import DataLoader

class TeamsOpposition:
    """
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.opposition_sql_path = "sql/oppositions/teams_x_teams"
        self.data_loader = DataLoader(postgres_to_dataframe)

    def build_oppositions(
        self,
        team: str,
        comps: list[str],
        seasons: list[str],
        side: str = 'both'
    ) -> pd.DataFrame:
        """Build Oppositions table between a teams and teams it played against.
        """

        seasons = [season.replace('-', '_') for season in seasons]

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

        self.db.execute_sql_file(f"{self.opposition_sql_path}/team_x_teams.sql")

        seasons = seasons if seasons else self.data_loader.get_seasons()
        comps = comps if comps else self.data_loader.get_competition_names()

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

    def build_matrix(
        self,
        stat: str,
        comp: str,
        season: str,
        side: str
    ) -> list:
        """
        """

        season = season.replace('-', '_')

        season_schema = f"season_{season}"

        self.db.execute_sql_file(f"{self.opposition_sql_path}/team_x_teams.sql")

        query = sql.SQL("""
            select c.name as "Club"
                from {}.team t
                join upper.club c
                on t.club = c.id
                where t.competition = %s
                group by c.name;
        """).format(sql.Identifier(season_schema))

        cursor_instance = self.db.execute_query(query, (comp,), return_cursor=True)

        if cursor_instance:
            teams = [row[0] for row in cursor_instance.fetchall()]
            cursor_instance.close()
            df = pd.DataFrame(index=teams, columns=teams)

            for team in teams:
                team_query = sql.SQL("""
                    select "Team", "Opponent", {stat}
                    from analytics.teams_oppositions(
                        team := %s,
                        side := %s
                    );
                """).format(stat=sql.Identifier(stat))

                cursor_oppositions = self.db.execute_query(
                    team_query,
                    (team, side),
                    return_cursor=True
                )

                data = cursor_oppositions.fetchall()
                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
