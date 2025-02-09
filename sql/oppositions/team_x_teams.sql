drop function if exists public.teams_oppositions;

create or replace function public.teams_oppositions(
    in team varchar(100),
    in id_comp varchar(20),
    in id_season varchar(20),
	in side ranking_type
)
returns table(
	"Team" varchar(100),
    "Opponent" varchar(100),
    "Matches" bigint,
    "Wins" bigint,
    "Draws" bigint,
    "Loses" bigint,
    "Goals For" bigint,
    "Goals Against" bigint,
    "Shots For" bigint,
    "Shots on Target For" bigint,
    "Shots Against" bigint,
    "Shots on Target Against" bigint,
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
                m.home_team as id_team,
                m.away_team as id_opponent,
                m.competition,

                tsh.score as home_goals_for,
                0 as away_goals_for,
                tsa.score as home_goals_against,
                0 as away_goals_against,

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

                tsh.nb_shots_total as home_shots_for,
                0 as away_shots_for,
                tsh.nb_shots_on_target as home_on_target_for,
                0 as away_on_target_for,

                0 as away_shots_against,
                tsa.nb_shots_total as home_shots_against,
                0 as away_on_target_against,
                tsa.nb_shots_on_target as home_on_target_against,

                tsh.nb_cards_yellow as home_y_cards,
                0 as away_y_cards,
                tsh.nb_cards_second_yellow as home_yr_cards,
                0 as away_yr_cards,
                tsh.nb_cards_red as home_r_cards,
                0 as away_r_cards

            from selected_match m
            join (select * from %I.team_stats where played_home) tsh
            on m.id = tsh.match
            join (select * from %I.team_stats where not played_home) tsa
            on m.id = tsa.match
        ),
        away_oppositions as (
            select
                m.away_team as id_team,
                m.home_team as id_opponent,
                m.competition,

                0 as home_goals_for,
                tsa.score as away_goals_for,
                0 as home_goals_against,
                tsh.score as away_goals_against,

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

                0 as home_shots_for,
                tsa.nb_shots_total as away_shots_for,
                0 as home_on_target_for,
                tsa.nb_shots_on_target as away_on_target_for,

                tsh.nb_shots_total as home_shots_against,
                0 as away_shots_against,
                tsh.nb_shots_on_target as home_on_target_against,
                0 as away_on_target_against,

                0 as home_y_cards,
                tsa.nb_cards_yellow as away_y_cards,
                0 as home_yr_cards,
                tsa.nb_cards_second_yellow as away_yr_cards,
                0 as home_r_cards,
                tsa.nb_cards_red as away_r_cards

            from selected_match m
            join (select * from %I.team_stats where played_home) tsh
            on m.id = tsh.match
            join (select * from %I.team_stats where not played_home) tsa
            on m.id = tsa.match
        ),
        unionised as (
            select *
            from home_oppositions h
            union all
            select * from away_oppositions a
        )
        select
            t.name as Team,
            o.name as Opponent,

            set_bigint_stat(sum(home_win + home_draw + home_lose), sum(away_win + away_draw + away_lose), ''' || side || ''') as Matches,
            set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
            set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
            set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,

            set_bigint_stat(sum(home_goals_for), sum(away_goals_for), ''' || side || ''') as "Goals For",
            set_bigint_stat(sum(home_goals_against), sum(away_goals_against), ''' || side || ''') as "Goals Against",

            set_bigint_stat(sum(home_shots_for), sum(away_shots_for), ''' || side || ''') as "Shots For",
            set_bigint_stat(sum(home_on_target_for), sum(away_on_target_for), ''' || side || ''') as "Shots on Target For",

            set_bigint_stat(sum(home_shots_against), sum(away_shots_against), ''' || side || ''') as "Shots Against",
            set_bigint_stat(sum(home_on_target_against), sum(away_on_target_against), ''' || side || ''') as "Shots on Target Against",

            set_bigint_stat(sum(home_y_cards), sum(away_y_cards), ''' || side || ''') as "Yellow Cards",
            set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
            set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards"

        from unionised u
        join dwh_upper.club t
        on u.id_team = u.competition || ''_'' || t.id 
        join dwh_upper.club o
        on u.id_opponent = u.competition || ''_'' || o.id
        where t.name = ''' || team || '''
        group by t.name, o.name;
        ',
        season_schema, season_schema,
        season_schema, season_schema,
        season_schema
    );

    RETURN QUERY EXECUTE query USING team, id_comp, id_season;
end;
$$ language plpgsql;