create or replace function analytics.teams_oppositions(
    in seasons varchar[],
    in comps varchar[],
    in team varchar(100),
	in side analytics.side
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

            analytics.set_bigint_stat(sum(tto.home_match), sum(tto.away_match), ''' || side || ''') as Matches,
            analytics.set_bigint_stat(sum(tto.home_win), sum(tto.away_win), ''' || side || ''') as Wins,
            analytics.set_bigint_stat(sum(tto.home_draw), sum(tto.away_draw), ''' || side || ''') as Draws,
            analytics.set_bigint_stat(sum(tto.home_lose), sum(tto.away_lose), ''' || side || ''') as Loses,

            analytics.set_bigint_stat(sum(tto.home_goals_for), sum(tto.away_goals_for), ''' || side || ''') as "Goals For",
            analytics.set_bigint_stat(sum(tto.home_goals_against), sum(tto.away_goals_against), ''' || side || ''') as "Goals Against",

            analytics.set_bigint_stat(sum(tto.home_shots_for), sum(tto.away_shots_for), ''' || side || ''') as "Shots For",
            analytics.set_bigint_stat(sum(tto.home_shots_ot_for), sum(tto.away_shots_ot_for), ''' || side || ''') as "Shots on Target For",

            analytics.set_bigint_stat(sum(tto.home_shots_against), sum(tto.away_shots_against), ''' || side || ''') as "Shots Against",
            analytics.set_bigint_stat(sum(tto.home_shots_ot_against), sum(tto.away_shots_ot_against), ''' || side || ''') as "Shots on Target Against",

            analytics.set_bigint_stat(sum(tto.home_y_cards), sum(tto.away_y_cards), ''' || side || ''') as "Yellow Cards",
            analytics.set_bigint_stat(sum(tto.home_yr_cards), sum(tto.away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
            analytics.set_bigint_stat(sum(tto.home_r_cards), sum(tto.away_r_cards), ''' || side || ''') as "Red Cards",

            case
                when count(distinct tto.competition) > 1 and grouping(tto.competition) = 1
                then ''TCC''
                else ''Competition''
            end as "Granularity Competition"

        from analytics.staging_teams_performance tto
        join upper.club c
        on tto.id_team = tto.id_comp || ''_'' || c.id 
        join upper.club o
        on tto.id_opponent = tto.id_comp || ''_'' || o.id
        where c.name = ''' || team || '''
            and tto.season = any($1)
            and tto.competition = any($2)
        group by grouping sets(
            (c.name, o.name, tto.competition),
            (c.name, o.name)
        )
        having grouping(tto.competition) = 0 OR count(distinct tto.competition) > 1;
        '
    );

    RETURN QUERY EXECUTE query USING seasons, comps;
end;
$$ language plpgsql;