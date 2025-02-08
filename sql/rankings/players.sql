/* TODO
 * (plus tard) Nombre de points rapportés à l'équipe en marquant
 * 
 * */
drop function if exists players_rankings;

create or replace function players_rankings(
	in id_comp varchar(100),
	in id_season varchar(20),
	in first_week int,
	in last_week int,
	in side ranking_type
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
	"Club" text,
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
	"Sub Out" bigint--,
	--"Last Opponent" varchar(100)
	--Attendance numeric,
)
as $$
DECLARE
    season_schema text;
	query text;
begin
	PERFORM check_parameters(id_comp, id_season, first_week, last_week, side);

	season_schema = 'dwh_' || id_season;
	
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
		when ps.Matches <> 0 then round(ps."xG Assists" / ps.Matches, 2)
		else 0.0
	end as "xG Assists /90",
	*/

	query := format(
		'with selected_match as (
			select id, home_team, away_team, competition
			from %I.match
			where 
				competition = ''' || id_comp || ''' 
				and length(week) <= 2 
				and cast(week as int) between ''' || first_week || ''' and ''' || last_week || '''
		),
		players_nationalities as (
			select
				player,
				array_agg(distinct country) as Nationalities
			from dwh_upper.player_nationality pn
			group by player
		),
		home_stats as (
			select
				1 as home_match,
				0 as away_match,

				pms.player,

				h.competition,

				h.home_team as team,

				case
					when pms.position = ''gk'' then 1 
					else 0
				end as home_gk,
				0 as away_gk,
				
				pms.nb_goals as home_goals,
				0 as away_goals,
				pms.nb_pens_scored as home_pens_made,
				0 as away_pens_made,
				pms.nb_assists as home_assists,
				0 as away_assists,
				pms.xg as home_xg,
				0.0 as away_xg,

				case
					when ts.score = 0 then 1 
					else 0
				end as home_clean_sheet,
				0 as away_clean_sheet,

				pms.nb_cards_yellow as home_cards_yellow,
				0 as away_cards_yellow,
				pms.nb_cards_red + pms.nb_cards_second_yellow as home_cards_red,
				0 as away_cards_red,
				pms.nb_cards_second_yellow as home_cards_yellow_red,
				0 as away_cards_yellow_red,

				case
					when ts.score > ts_away.score then 1 
					else 0
				end as home_win,
				0 as away_win,

				case
					when ts.score = ts_away.score then 1 
					else 0
				end as home_draw,
				0 as away_draw,

				case
					when ts.score < ts_away.score then 1 
					else 0
				end as home_lose,
				0 as away_lose,

				case
					when ts.captain = pms.player then 1 
					else 0
				end as home_captain,
				0 as away_captain,

				pms.nb_minutes as home_minutes,
				0 as away_minutes,

				case
					when c.started then 1 
					else 0
				end as home_started,
				0 as away_started,
				
				case
					when not c.started then 1 
					else 0
				end as home_sub_in,
				0 as away_sub_in,
				
				case
					when e.player_out = c.player then 1 
					else 0
				end as home_sub_out,
				0 as away_sub_out
			from selected_match as h
			join (select * from %I.player_main_stats where played_home) as pms
			on pms.match = h.id
			join (select match, team, captain, score from %I.team_stats where played_home) as ts 
			on h.id = ts.match and ts.team = h.home_team
			join (select match, team, captain, score from %I.team_stats where not played_home) as ts_away
			on h.id = ts_away.match
			join (select * from %I.compo where played_home) as c
			on h.id = c.match and pms.player = c.player
			left join (select match, team, player_in, player_out from %I.event e join %I.sub_event se on e.id = se.id where e.played_home) as e
			on h.id = e.match and (e.player_in = c.player or e.player_out = c.player)
		),
		away_stats as (
			select
				0 as home_match,
				1 as away_match,

				pms.player,

				a.competition,

				a.away_team as team,

				0 as home_gk,
				case
					when pms.position = ''gk'' then 1 else 0
				end as away_gk,
				
				0 as home_goals,
				pms.nb_goals as away_goals,
				0 as home_pens_made,
				pms.nb_pens_scored as away_pens_made,
				0 as home_assists,
				pms.nb_assists as away_assists,
				0.0 as home_xg,
				pms.xg as away_xg,

				0 as home_clean_sheet,
				case
					when ts.score = 0 then 1 else 0
				end as away_clean_sheet,

				0 as home_cards_yellow,
				pms.nb_cards_yellow as away_cards_yellow,
				0 as home_cards_red,
				pms.nb_cards_red + pms.nb_cards_second_yellow as away_cards_red,
				0 as home_cards_yellow_red,
				pms.nb_cards_yellow as away_cards_yellow_red,

				0 as home_win,
				case
					when ts_home.score > ts.score then 1 else 0
				end as away_win,

				0 as home_draw,
				case
					when ts_home.score = ts.score then 1 else 0
				end as away_draw,

				0 as home_lose,
				case
					when ts_home.score < ts.score then 1 else 0
				end as away_lose,

				0 as home_captain,
				case
					when ts.captain = pms.player then 1 else 0
				end as away_captain,

				0 as home_minutes,
				pms.nb_minutes as away_minutes,

				0 as home_started,
				case
					when c.started then 1 else 0
				end as away_started,
				
				0 as home_sub_in,
				case
					when not c.started then 1 else 0
				end as away_sub_in,
				
				0 as home_sub_out,
				case
					when e.player_out = c.player then 1 else 0
				end as away_sub_out
			from selected_match as a
			join (select * from %I.player_main_stats where not played_home) as pms
			on pms.match = a.id
			join (select match, team, captain, score from %I.team_stats where not played_home) as ts 
			on a.id = ts.match and ts.team = a.away_team
			join (select match, team, captain, score from %I.team_stats where played_home) as ts_home
			on a.id = ts_home.match
			join (select * from %I.compo where not played_home) c
			on a.id = c.match and pms.player = c.player
			left join (select match, team, player_in, player_out from %I.event e join %I.sub_event se on e.id = se.id where not e.played_home) as e
			on a.id = e.match and (e.player_in = c.player or e.player_out = c.player)
		),
		players_stats as (
			select
				stats.player,
				pn.Nationalities,

				case
					when set_bigint_stat(sum(home_gk), sum(away_gk), ''' || side || ''') > 0 then true
					else false
				end as GK,
				
				string_agg(distinct c.name, '', '') as Club,

				set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,

				set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
				set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
				set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,
				
				set_bigint_stat(sum(home_goals), sum(away_goals), ''' || side || ''') as Goals,
				set_bigint_stat(sum(home_pens_made), sum(away_pens_made), ''' || side || ''') as Penalties,
				set_bigint_stat(sum(home_assists), sum(away_assists), ''' || side || ''') as Assists,

				set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, ''' || side || ''') as xG,

				set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), ''' || side || ''') as "Clean Sheets",
				
				set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), ''' || side || ''') as "Yellow Cards",
				set_bigint_stat(sum(home_cards_red), sum(away_cards_red), ''' || side || ''') as "Red Cards",
				set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), ''' || side || ''') as "Incl. 2 Yellow Cards",
				
				set_bigint_stat(sum(home_minutes), sum(away_minutes), ''' || side || ''') as Minutes,

				set_bigint_stat(sum(home_captain), sum(away_captain), ''' || side || ''') as Captain,

				set_bigint_stat(sum(home_started), sum(away_started), ''' || side || ''') as Started,
				set_bigint_stat(sum(home_sub_in), sum(away_sub_in), ''' || side || ''') as "Sub In",
				set_bigint_stat(sum(home_sub_out), sum(away_sub_out), ''' || side || ''') as "Sub Out"
				
			from (
				select *
				from home_stats
				union all
				select *
				from away_stats
			) as "stats"
			join (select id, name from dwh_upper.club) as c 
			on team = competition || ''_'' || c.id
			join players_nationalities pn
			on stats.player = pn.player
			group by stats.player, pn.Nationalities
		)
		select 
			p.name as Player,

			EXTRACT(YEAR FROM age(current_date, birth_date))::bigint AS Age,

			p.height::bigint as Height,
			p.weight::bigint as Weight,

			p.strong_foot as Footed,
			
			ps.Nationalities as Nationalities,

			ps.GK,

			ps.Club,

			ps.Matches,

			ps.Wins,
			ps.Draws,
			ps.Loses,

			ps.Goals,
			ps.Penalties,
			ps.Assists,

			ps.xG,
			case
				when ps.Matches <> 0 then round(ps.xG / ps.Matches, 2)
				else 0.0
			end as "xG/90",

			ps."Clean Sheets",

			ps."Yellow Cards",
			ps."Red Cards",
			ps."Incl. 2 Yellow Cards",

			ps.Minutes,
			case
				when ps.Matches <> 0 then round(ps.Minutes / ps.Matches, 2)
				else 0.0
			end as "Minutes/Match",

			ps.Captain,

			ps.Started,
			ps."Sub In",
			ps."Sub Out"
		from players_stats ps
		join dwh_upper.player p
		on ps.player = p.id
		',
		season_schema, season_schema, 
		season_schema, season_schema, 
		season_schema, season_schema, 
		season_schema, season_schema, 
		season_schema, season_schema, 
		season_schema, season_schema,
		season_schema, season_schema,
		season_schema, season_schema
	);
	
	RETURN QUERY EXECUTE query USING id_comp, id_season, first_week, last_week, side;
end;
$$ language plpgsql;
