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
                when count(distinct tp.competition) > 1 and grouping(tp.competition) = 1
                then array_agg(distinct tp.competition)
                else array[tp.competition]
            end as Competitions,

            analytics.set_bigint_stat(sum(tp.home_match), sum(tp.away_match), ''' || side || ''') as Matches,
            analytics.set_bigint_stat(sum(tp.home_win), sum(tp.away_win), ''' || side || ''') as Wins,
            analytics.set_bigint_stat(sum(tp.home_draw), sum(tp.away_draw), ''' || side || ''') as Draws,
            analytics.set_bigint_stat(sum(tp.home_lose), sum(tp.away_lose), ''' || side || ''') as Loses,

            analytics.set_bigint_stat(sum(tp.home_goals_for), sum(tp.away_goals_for), ''' || side || ''') as "Goals For",
            analytics.set_bigint_stat(sum(tp.home_goals_against), sum(tp.away_goals_against), ''' || side || ''') as "Goals Against",

            analytics.set_bigint_stat(sum(tp.home_shots_for), sum(tp.away_shots_for), ''' || side || ''') as "Shots For",
            analytics.set_bigint_stat(sum(tp.home_shots_ot_for), sum(tp.away_shots_ot_for), ''' || side || ''') as "Shots on Target For",

            analytics.set_bigint_stat(sum(tp.home_shots_against), sum(tp.away_shots_against), ''' || side || ''') as "Shots Against",
            analytics.set_bigint_stat(sum(tp.home_shots_ot_against), sum(tp.away_shots_ot_against), ''' || side || ''') as "Shots on Target Against",

            analytics.set_bigint_stat(sum(tp.home_y_cards), sum(tp.away_y_cards), ''' || side || ''') as "Yellow Cards",
            analytics.set_bigint_stat(sum(tp.home_yr_cards), sum(tp.away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
            analytics.set_bigint_stat(sum(tp.home_r_cards), sum(tp.away_r_cards), ''' || side || ''') as "Red Cards",

            case
                when count(distinct tp.competition) > 1 and grouping(tp.competition) = 1
                then ''TCC''
                else ''Competition''
            end as "Granularity Competition"

        from analytics.staging_teams_performance tp
        join upper.club c
        on tp.id_team = tp.id_comp || ''_'' || c.id 
        join upper.club o
        on tp.id_opponent = tp.id_comp || ''_'' || o.id
        where c.name = ''' || team || '''
            and tp.season = any($1)
            and tp.competition = any($2)
        group by grouping sets(
            (c.name, o.name, tp.competition),
            (c.name, o.name)
        )
        having grouping(tp.competition) = 0 OR count(distinct tp.competition) > 1;
        '
    );

    RETURN QUERY EXECUTE query USING seasons, comps;
end;
$$ language plpgsql;