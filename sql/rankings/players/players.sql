create or replace function analytics.players_rankings(
	in side analytics.ranking_type,
	in r int
)
returns table(
	--"Ranking" bigint,
	"Player" varchar(100),
	"Age" bigint,
	"Height" bigint,
	"Weight" bigint,
	"Footed" varchar(20),
	"Nationalities" varchar[],
	"GK" bool,
	"Clubs" varchar[],
	"Competitions" varchar[],
	"Matches" bigint,
	"Wins" bigint,
	"Draws" bigint,
	"Loses" bigint,
	"Goals" bigint,
	"Penalties" bigint,
	"Assists" bigint,
	"xG" numeric,
	"xG/90" numeric,
	--"xG Assists" numeric,
	--"xG Assists /90" numeric,
	"Clean Sheets" bigint,
	"Yellow Cards" bigint,
	"Red Cards" bigint,
	"Incl. 2 Yellow Cards" bigint,
	"Minutes" bigint,
	"Minutes/Match" numeric,
	"Captain" bigint,
	"Started" bigint,
	"Sub In" bigint,
	"Sub Out" bigint
	-- "Granularity" varchar(100)--,
	--"Last Opponent" varchar(100)
	--Attendance numeric,
)
as $$
DECLARE
	query text;
begin

	query := format(
		'with players_nationalities as (
			select
				player,
				array_agg(distinct country) as Nationalities
			from upper.player_nationality pn
			group by player
		),
		players_stats as (
			select
				stats.player,
				pn.Nationalities,

				case
					when analytics.set_bigint_stat(sum(home_gk), sum(away_gk), ''' || side || ''') > 0 then true
					else false
				end as GK,
				
				case
					when count(distinct c.name) > 1 and grouping(c.name) = 1 
					then array_agg(distinct c.name)
					else array[c.name]
				end as Clubs,

				case
					when count(distinct stats.competition) > 1 and grouping(stats.competition) = 1
					then array_agg(distinct stats.competition)
					else array[stats.competition]
				end as Competitions,

				analytics.set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,

				analytics.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
				analytics.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
				analytics.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,
				
				analytics.set_bigint_stat(sum(home_goals), sum(away_goals), ''' || side || ''') as Goals,
				analytics.set_bigint_stat(sum(home_pens_made), sum(away_pens_made), ''' || side || ''') as Penalties,
				analytics.set_bigint_stat(sum(home_assists), sum(away_assists), ''' || side || ''') as Assists,

				analytics.set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, ''' || side || ''') as xG,

				analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), ''' || side || ''') as "Clean Sheets",
				
				analytics.set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), ''' || side || ''') as "Yellow Cards",
				analytics.set_bigint_stat(sum(home_cards_red), sum(away_cards_red), ''' || side || ''') as "Red Cards",
				analytics.set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), ''' || side || ''') as "Incl. 2 Yellow Cards",
				
				analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), ''' || side || ''') as Minutes,

				analytics.set_bigint_stat(sum(home_captain), sum(away_captain), ''' || side || ''') as Captain,

				analytics.set_bigint_stat(sum(home_started), sum(away_started), ''' || side || ''') as Started,
				analytics.set_bigint_stat(sum(home_sub_in), sum(away_sub_in), ''' || side || ''') as "Sub In",
				analytics.set_bigint_stat(sum(home_sub_out), sum(away_sub_out), ''' || side || ''') as "Sub Out"
	
			from tmp_players_ranking as stats
			join (select id, name from upper.club) as c 
			on team = id_comp || ''_'' || c.id
			join players_nationalities pn
			on stats.player = pn.player
			group by
				(stats.player, pn.Nationalities),
				cube(c.name, stats.competition)
			having (
				grouping(c.name) = 0 OR count(distinct c.name) > 1
			) and (
				grouping(stats.competition) = 0 OR count(distinct stats.competition) > 1
			)
		)
		select 
			p.name as Player,

			EXTRACT(YEAR FROM age(current_date, birth_date))::bigint AS Age,

			p.height::bigint as Height,
			p.weight::bigint as Weight,

			p.strong_foot as Footed,
			
			ps.Nationalities as Nationalities,

			ps.GK,

			ps.Clubs,

			ps.Competitions,

			ps.Matches,

			ps.Wins,
			ps.Draws,
			ps.Loses,

			ps.Goals,
			ps.Penalties,
			ps.Assists,

			ps.xG,
			case
				when ps.Matches <> 0 then round(ps.xG / ps.Matches, ''' || r || ''')
				else 0.0
			end as "xG/90",

			ps."Clean Sheets",

			ps."Yellow Cards",
			ps."Red Cards",
			ps."Incl. 2 Yellow Cards",

			ps.Minutes,
			case
				when ps.Matches <> 0 then round(ps.Minutes / ps.Matches, ''' || r || ''')
				else 0.0
			end as "Minutes/Match",

			ps.Captain,

			ps.Started,
			ps."Sub In",
			ps."Sub Out"

		from players_stats ps
		join upper.player p
		on ps.player = p.id;
		'
	);
	
	RETURN QUERY EXECUTE query USING side, r;
end;
$$ language plpgsql;
