create or replace function dwh_utils.players_oppositions(
    in player varchar(100),
    in id_comp varchar(20),
    in id_season varchar(20),
	in side dwh_utils.ranking_type
)
returns table(
	"Player" varchar(100),
    "Opponent" varchar(100),
    "Matches" bigint,
    "Wins" bigint,
    "Draws" bigint,
    "Loses" bigint,
    "Goals" bigint,
    "Assists" bigint,
    "Minutes" bigint,
    "Minutes/Match" numeric,
    "Shots" bigint,
    "Shots on Target" bigint,
    "Yellow Cards" bigint,
    "Incl. 2 Yellow Cards" bigint,
    "Red Cards" bigint
)
as $$
DECLARE
    season_schema text;
	query text;
begin
    season_schema = 'dwh_' || id_season;

    query := format(
        'with selected_match as (
			select id, home_team, away_team, attendance, competition
			from %I.match 
			where competition = ''' || id_comp || '''
		),
        home_oppositions as (
            select
                ps.player,
                m.competition,

                m.away_team as opponent,

                case
                    when tsh.score > tsa.score then 1
                    else 0
                end as home_win,
                0 as away_win,
                case
                    when tsh.score = tsa.score then 1
                    else 0
                end as home_draw,
                0 as away_draw,
                case
                    when tsh.score < tsa.score then 1
                    else 0
                end as home_lose,
                0 as away_lose,

                ps.nb_goals as home_goals,
                0 as away_goals,

                ps.nb_assists as home_assists,
                0 as away_assists,

                ps.nb_minutes as home_minutes,
                0 as away_minutes,

                ps.nb_shots as home_shots,
                0 as away_shots,

                ps.nb_shots_on_target as home_on_target,
                0 as away_on_target,

                ps.nb_cards_yellow as home_y_cards,
                0 as away_y_cards,
                ps.nb_cards_second_yellow as home_yr_cards,
                0 as away_yr_cards,
                ps.nb_cards_red as home_r_cards,
                0 as away_r_cards
            
            from (select * from %I.player_main_stats where played_home) as ps
            join selected_match m
            on ps.match = m.id
            join (select * from %I.team_stats where played_home) as tsh
            on tsh.match = m.id
            join (select * from %I.team_stats where not played_home) as tsa
            on tsa.match = m.id
        ),
        away_oppositions as (
            select
                ps.player,
                m.competition,

                m.home_team as opponent,

                0 as home_win,
                case
                    when tsa.score > tsh.score then 1
                    else 0
                end as away_win,

                0 as home_draw,
                case
                    when tsa.score = tsh.score then 1
                    else 0
                end as away_draw,

                0 as home_lose,
                case
                    when tsa.score < tsh.score then 1
                    else 0
                end as away_lose,

                0 as home_goals,
                ps.nb_goals as away_goals,

                0 as home_assists,
                ps.nb_assists as away_assists,

                0 as home_minutes,
                ps.nb_minutes as away_minutes,

                0 as homme_shots,
                ps.nb_shots as away_shots,

                0 as home_on_target,
                ps.nb_shots_on_target as away_on_target,

                0 as home_y_cards,
                ps.nb_cards_yellow as away_y_cards,
                0 as home_yr_cards,
                ps.nb_cards_second_yellow as away_yr_cards,
                0 as home_r_cards,
                ps.nb_cards_red as away_r_cards
            
            from (select * from %I.player_main_stats where not played_home) as ps
            join selected_match m
            on ps.match = m.id
            join (select * from %I.team_stats where played_home) as tsh
            on tsh.match = m.id
            join (select * from %I.team_stats where not played_home) as tsa
            on tsa.match = m.id
        ),
        unionised as (
            select *
            from home_oppositions
            union all
            select *
            from away_oppositions
        ),
        pre_matrix as (
            select
                p.name as Player,
                c.name as Opponent,

                dwh_utils.set_bigint_stat(sum(home_win + home_draw + home_lose), sum(away_win + away_draw + away_lose), ''' || side || ''') as Matches,
                dwh_utils.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
                dwh_utils.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
                dwh_utils.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,

                dwh_utils.set_bigint_stat(sum(home_goals), sum(away_goals), ''' || side || ''') as Goals,
                dwh_utils.set_bigint_stat(sum(home_assists), sum(away_assists), ''' || side || ''') as Assists,
                
                dwh_utils.set_bigint_stat(sum(home_minutes), sum(away_minutes), ''' || side || ''') as Minutes,
                
                dwh_utils.set_bigint_stat(sum(home_shots), sum(away_shots), ''' || side || ''') as Shots,
                dwh_utils.set_bigint_stat(sum(home_on_target), sum(away_on_target), ''' || side || ''') as "Shots on Target",
                dwh_utils.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), ''' || side || ''') as "Yellow Cards",
                dwh_utils.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
                dwh_utils.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards"
            
            from unionised u
            join dwh_upper.player p
            on u.player = p.id
            join dwh_upper.club c
            on u.opponent = u.competition || ''_'' || c.id
            where p.name = ''' || player || '''
            group by p.name, c.name
        )
        select
            Player,
            Opponent,
            Matches,
            Wins,
            Draws,
            Loses,
            Goals,
            Assists,
            Minutes,
            case
                when Matches != 0
                then round(Minutes / Matches, 2)
                else 0.0
            end as"Minutes/Match",
            Shots,
            "Shots on Target",
            "Yellow Cards",
            "Incl. 2 Yellow Cards",
            "Red Cards"
        from pre_matrix;
        ',
        season_schema, season_schema,
        season_schema, season_schema,
        season_schema, season_schema,
        season_schema
    );

    RETURN QUERY EXECUTE query USING id_comp, id_season;
end;
$$ language plpgsql;