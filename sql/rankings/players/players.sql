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
	"Sub Out" bigint,
	"Granularity" varchar(100)--,
	--"Last Opponent" varchar(100)
	--Attendance numeric,
)
as $$
DECLARE
	query text;
begin
	
	/*with ranked_positions as (
		SELECT id_player, "position", ROW_NUMBER() OVER (PARTITION BY id_player ORDER BY COUNT(*) DESC) AS "position_rank"
		FROM player_stats
		GROUP BY id_player, "position"
	)*/

	/*
	home_compo as (
		select
			m.id,
			c.player,
			c.started,
			case 
				when not c.started then 1
				else 0
			end as sub_in,
			case
				when e.player_out = c.player then 1
				else 0
			end as sub_out
		from (select * from %I.compo where played_home) as c
		join selected_match m
		on c.match = m.id
		join player_main_stats pms
		on c.id_match = pms.match and pms.player = c.player
		left join (select id, match from %I.event where played_home) as e
		on m.id = e.match
		left join %I.sub_event se
		on c.player = se.player_out
	),
	*/

	/*set_numeric_stat(sum(home_xg_assists)::numeric, sum(away_xg_assists)::numeric, ''' || side || ''') as "xG Assists",*/

	/*
	ps."xG Assists",
	case
		when ps.Matches <> 0 then round(ps."xG Assists" / ps.Matches, ''' || r || ''')
		else 0.0
	end as "xG Assists /90",
	*/

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
				
			from tmp_players_ranking as "stats"
			join (select id, name from upper.club) as c 
			on team = competition || ''_'' || c.id
			join players_nationalities pn
			on stats.player = pn.player
			group by grouping sets(
				(stats.player, pn.Nationalities, c.name),
				(stats.player, pn.Nationalities)
			)
			having grouping(c.name) = 0 OR count(distinct c.name) > 1
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
			ps."Sub Out",

			case
				when array_length(ps.Clubs, 1) > 1 then ''Total''
				else ''Club''
			end::varchar as Granularity
		from players_stats ps
		join upper.player p
		on ps.player = p.id
		'
	);
	
	RETURN QUERY EXECUTE query USING side, r;
end;
$$ language plpgsql;
