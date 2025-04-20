create or replace function analytics.teams_oppositions(
    in team varchar(100),
	in side analytics.ranking_type
)
returns table(
	"Team" varchar(100),
    "Opponent" varchar(100),
    "Competitions" varchar[],
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
    "Red Cards" bigint,
    "Granularity Competition" text
)
as $$
DECLARE
	query text;
begin

    query := format(
        '
        select
            c.name as Team,
            o.name as Opponent,

            case
                when count(distinct tto.competition) > 1 and grouping(tto.competition) = 1
                then array_agg(distinct tto.competition)
                else array[tto.competition]
            end as Competitions,

            analytics.set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,
            analytics.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
            analytics.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
            analytics.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,

            analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), ''' || side || ''') as "Goals For",
            analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), ''' || side || ''') as "Goals Against",

            analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), ''' || side || ''') as "Shots For",
            analytics.set_bigint_stat(sum(home_on_target_for), sum(away_on_target_for), ''' || side || ''') as "Shots on Target For",

            analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), ''' || side || ''') as "Shots Against",
            analytics.set_bigint_stat(sum(home_on_target_against), sum(away_on_target_against), ''' || side || ''') as "Shots on Target Against",

            analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), ''' || side || ''') as "Yellow Cards",
            analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
            analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards",

            case
                when count(distinct tto.competition) > 1 and grouping(tto.competition) = 1
                then ''TCC''
                else ''Competition''
            end as "Granularity Competition"

        from tmp_teams_opposition tto
        join upper.club c
        on tto.team = tto.id_comp || ''_'' || c.id 
        join upper.club o
        on tto.opponent = tto.id_comp || ''_'' || o.id
        where c.name = ''' || team || '''
        group by grouping sets(
            (c.name, o.name, tto.competition),
            (c.name, o.name)
        )
        having grouping(tto.competition) = 0 OR count(distinct tto.competition) > 1;
        '
    );

    RETURN QUERY EXECUTE query USING team, side;
end;
$$ language plpgsql;