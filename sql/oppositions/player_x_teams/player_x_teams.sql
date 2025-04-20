create or replace function analytics.players_oppositions(
    in player varchar(100),
	in side analytics.ranking_type
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
                    when count(distinct tpo.competition) > 1 and grouping(tpo.competition) = 1
                    then array_agg(distinct tpo.competition)
                    else array[tpo.competition]
                end as Competitions,

                analytics.set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,
                analytics.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
                analytics.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
                analytics.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,

                analytics.set_bigint_stat(sum(home_goals), sum(away_goals), ''' || side || ''') as Goals,
                analytics.set_bigint_stat(sum(home_assists), sum(away_assists), ''' || side || ''') as Assists,
                
                analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), ''' || side || ''') as Minutes,
                
                analytics.set_bigint_stat(sum(home_shots), sum(away_shots), ''' || side || ''') as Shots,
                analytics.set_bigint_stat(sum(home_on_target), sum(away_on_target), ''' || side || ''') as "Shots on Target",
                analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), ''' || side || ''') as "Yellow Cards",
                analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
                analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards",

                case
					when count(distinct c.name) > 1 and grouping(c.name) = 1 
					then ''TCC''
					else ''Club''
				end as "Granularity Club",

                case
                    when count(distinct tpo.competition) > 1 and grouping(tpo.competition) = 1
                    then ''TCC''
                    else ''Competition''
                end as "Granularity Competition"
            
            from tmp_players_opposition tpo
            join upper.player p
            on tpo.player = p.id
            join upper.club c
            on tpo.team = tpo.id_comp || ''_'' || c.id
            join upper.club o
            on tpo.opponent = tpo.id_comp || ''_'' || o.id
            where p.name = ''' || player || '''
            group by 
                (p.name, o.name),
                cube(c.name, tpo.competition)
            having (
                grouping(c.name) = 0 OR count(distinct c.name) > 1
            ) and (
				grouping(tpo.competition) = 0 OR count(distinct tpo.competition) > 1
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

    RETURN QUERY EXECUTE query USING player, side;
end;
$$ language plpgsql;