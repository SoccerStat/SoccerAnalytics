create or replace function analytics.players_oppositions(
    in seasons varchar[],
    in comps varchar[],
    in player varchar(100),
	in side analytics.side
)
returns table(
	"Player" varchar(100),
    "Clubs" varchar[],
    "Opponent" varchar(100),
    "Competitions" varchar[],
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
    "Red Cards" bigint,
    "Granularity Club" text,
    "Granularity Competition" text
)
as $$
DECLARE
	query text;
begin

    query := format(
        'with pre_matrix as (
            select
                p.name as Player,
                case
					when count(distinct c.name) > 1 and grouping(c.name) = 1 
					then array_agg(distinct c.name)
					else array[c.name]
				end as Clubs,

                o.name as Opponent,

                case
                    when count(distinct pp.competition) > 1 and grouping(pp.competition) = 1
                    then array_agg(distinct pp.competition)
                    else array[pp.competition]
                end as Competitions,

                analytics.set_bigint_stat(sum(pp.home_match), sum(pp.away_match), ''' || side || ''') as Matches,
                analytics.set_bigint_stat(sum(pp.home_win), sum(pp.away_win), ''' || side || ''') as Wins,
                analytics.set_bigint_stat(sum(pp.home_draw), sum(pp.away_draw), ''' || side || ''') as Draws,
                analytics.set_bigint_stat(sum(pp.home_lose), sum(pp.away_lose), ''' || side || ''') as Loses,

                analytics.set_bigint_stat(sum(pp.home_goals), sum(pp.away_goals), ''' || side || ''') as Goals,
                analytics.set_bigint_stat(sum(pp.home_assists), sum(pp.away_assists), ''' || side || ''') as Assists,
                
                analytics.set_bigint_stat(sum(pp.home_minutes), sum(pp.away_minutes), ''' || side || ''') as Minutes,
                
                analytics.set_bigint_stat(sum(pp.home_shots), sum(pp.away_shots), ''' || side || ''') as Shots,
                analytics.set_bigint_stat(sum(pp.home_shots_ot), sum(pp.away_shots_ot), ''' || side || ''') as "Shots on Target",

                analytics.set_bigint_stat(sum(pp.home_y_cards), sum(pp.away_y_cards), ''' || side || ''') as "Yellow Cards",
                analytics.set_bigint_stat(sum(pp.home_yr_cards), sum(pp.away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
                analytics.set_bigint_stat(sum(pp.home_r_cards), sum(pp.away_r_cards), ''' || side || ''') as "Red Cards",

                case
					when count(distinct c.name) > 1 and grouping(c.name) = 1 
					then ''TCC''
					else ''Club''
				end as "Granularity Club",

                case
                    when count(distinct pp.competition) > 1 and grouping(pp.competition) = 1
                    then ''TCC''
                    else ''Competition''
                end as "Granularity Competition"
            
            from analytics.staging_players_performance pp
            join upper.player p
            on pp.id_player = p.id
            join upper.club c
            on pp.id_team = pp.id_comp || ''_'' || c.id
            join upper.club o
            on pp.id_opponent = pp.id_comp || ''_'' || o.id
            where p.name = ''' || player || '''
                and pp.season = any($1)
                and pp.competition = any($2)
            group by 
                (p.name, o.name),
                cube(c.name, pp.competition)
            having (
                grouping(c.name) = 0 OR count(distinct c.name) > 1
            ) and (
				grouping(pp.competition) = 0 OR count(distinct pp.competition) > 1
			)
        )
        select
            Player,
            Clubs,
            Opponent,
            Competitions,
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
            "Red Cards",
            "Granularity Club",
            "Granularity Competition"
        from pre_matrix;
        '
    );

    RETURN QUERY EXECUTE query USING seasons, comps;
end;
$$ language plpgsql;