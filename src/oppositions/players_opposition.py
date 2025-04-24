import pandas as pd
from psycopg2 import sql

from src.postgres.postgres_querying import PostgresQuerying
from src.utils.data_loader import DataLoader

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresQuerying):
        self.db = postgres_to_dataframe
        self.opposition_sql_path = "sql/oppositions/players_x_teams"
        self.data_loader = DataLoader(postgres_to_dataframe)
        self.db.execute_sql_file(f"{self.opposition_sql_path}/player_x_teams.sql")

    def build_oppositions(
        self,
        player: str,
        seasons: list[str],
        comps: list[str],
        side: str = "both"
    ) -> pd.DataFrame:
        """Build Oppositions table between a player and teams he played against.
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
            from analytics.players_oppositions(
                seasons := %s,
                comps := %s,
                player := %s,
                side := %s
            ) as "po"
            where "po"."Matches" != 0;
        """)

        return self.db.df_from_query(
            query,
            (seasons, comps, player.replace("'", "''"), side)
        )

    def build_oppositions_wrapper(
        self,
        player: str,
        seasons: tuple[str],
        comps: tuple[str],
        side=str
    ):
        return self.build_oppositions(
            player=player,
            seasons=list(seasons),
            comps=list(comps),
            side=side
        )

    def build_matrix(
        self,
        stat: str,
        seasons: list[str],
        comps: list[str],
        side: str
    ) -> pd.DataFrame:
        """
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

        all_players = []
        all_teams = []
        for season in seasons:
            for comp in comps:
                query = sql.SQL("""
                    select p.name as "Player"
                    from upper.player p
                    join {}.team_player tp
                    on tp.player = p.id
                    join {}.team t
                    on tp.team = t.id
                    left join upper.championship chp
                    on t.competition = chp.id
                    left join upper.continental_cup c_cup
                    on t.competition = c_cup.id
                    where %s in (chp.name, c_cup.name)
                    group by p.name;
                """).format(sql.Identifier(f"season_{season}"), sql.Identifier(f"season_{season}"))

                cursor_players = self.db.execute_query(query, (comp,), return_cursor=True)
                all_players += [row[0] for row in cursor_players.fetchall() if row[0] not in all_players]
                cursor_players.close()

                teams_query = sql.SQL("""
                    select c.name as "Club"
                    from {}.team t
                    join upper.club c
                    on t.id = t.competition || '_' || c.id
                    where t.competition = %s
                    group by c.name;
                """).format(sql.Identifier(f"season_{season}"))

                cursor_teams = self.db.execute_query(teams_query, (comp,), return_cursor=True)
                all_teams += [row[0] for row in cursor_teams.fetchall() if row[0] not in all_teams]
                cursor_teams.close()

        df = pd.DataFrame(index=all_players, columns=all_teams)

        for player in all_players:
            player_query = sql.SQL("""
                select "Player", "Opponent", {}
                from analytics.players_oppositions(
                    seasons := %s,
                    comps := %s,
                    player := %s,
                    side := %s
                );
            """).format(sql.Identifier(stat))

            cursor_oppositions = self.db.execute_query(
                player_query,
                (seasons, comps, player.replace("'", "''"), side),
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
