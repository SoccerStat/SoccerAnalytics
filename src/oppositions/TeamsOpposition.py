import pandas as pd

from psycopg2 import sql

from postgres.PostgresQuerying import PostgresQuerying

class TeamsOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe

    def build_oppositions(
        self,
        team: str,
        id_comp: str,
        id_season: str,
        side: str = 'both'
    ) -> pd.DataFrame:

        self.db.execute_sql_file("sql/rankings/sub_functions.sql")
        self.db.execute_sql_file("sql/oppositions/team_x_teams.sql")

        query = sql.SQL("""
            select * 
            from dwh_utils.teams_oppositions(
                team := %s,
                id_comp := %s,
                id_season := %s,
                side := %s
                ) as "to"
            where "to"."Matches" != 0;
        """)

        return self.db.df_from_query(query, (team, id_comp, id_season.replace('-', '_'), side))

    def build_matrix(
        self,
        stat: str,
        id_comp: str,
        season: str,
        side: str
    ) -> list:

        season = season.replace('-', '_')

        season_schema = f"dwh_{season}"

        self.db.execute_sql_file("sql/oppositions/team_x_teams.sql")

        query = sql.SQL("""
            select c.name as "Club"
                from {}.team t
                join dwh_upper.club c
                on t.club = c.id
                where t.competition = %s
                group by c.name;
        """).format(sql.Identifier(season_schema))

        cursor_instance = self.db.execute_query_get_cursor(query, (id_comp,))

        if cursor_instance:
            teams = [row[0] for row in cursor_instance.fetchall()]
            cursor_instance.close()
            df = pd.DataFrame(index=teams, columns=teams)

            for team in teams:
                team_query = sql.SQL("""
                    select "Team", "Opponent", {stat}
                    from dwh_utils.teams_oppositions(
                        team := %s,
                        id_comp := %s,
                        id_season := %s,
                        side := %s
                    );
                """).format(stat=sql.Identifier(stat))

                cursor_oppositions = self.db.execute_query_get_cursor(team_query,
                    (team, id_comp, season, side)
                )

                data = cursor_oppositions.fetchall()
                cursor_oppositions.close()

                for stats in data:
                    df.loc[stats[0], stats[1]] = stats[2]

            return df

        return pd.DataFrame()
