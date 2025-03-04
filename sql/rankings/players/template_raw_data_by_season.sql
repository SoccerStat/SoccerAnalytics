
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
		from (select * from dwh_{season}.compo where played_home) as c
		join selected_match m
		on c.match = m.id
		join player_main_stats pms
		on c.id_match = pms.match and pms.player = c.player
		left join (select id, match from dwh_{season}.event where played_home) as e
		on m.id = e.match
		left join dwh_{season}.sub_event se
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

with selected_match as (
	select id, home_team, away_team, competition
	from dwh_{season}.match
	where 
		competition = '{id_comp}'
		and length(week) <= 2 
		and cast(week as int) between '{first_week}' and '{last_week}'
),
home_stats as (
	select
		'{season}' as season,
		1 as home_match,
		0 as away_match,

		pms.player,

		h.competition,

		h.home_team as team,

		case
			when pms.position = 'gk' then 1 
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
	join (select * from dwh_{season}.player_main_stats where played_home) as pms
	on pms.match = h.id
	join (select match, team, captain, score from dwh_{season}.team_stats where played_home) as ts 
	on h.id = ts.match and ts.team = h.home_team
	join (select match, team, captain, score from dwh_{season}.team_stats where not played_home) as ts_away
	on h.id = ts_away.match
	join (select * from dwh_{season}.compo where played_home) as c
	on h.id = c.match and pms.player = c.player
	left join (select match, team, player_in, player_out from dwh_{season}.event e join dwh_{season}.sub_event se on e.id = se.id where e.played_home) as e
	on h.id = e.match and (e.player_in = c.player or e.player_out = c.player)
),
away_stats as (
	select
		'{season}' as season,
		0 as home_match,
		1 as away_match,

		pms.player,

		a.competition,

		a.away_team as team,

		0 as home_gk,
		case
			when pms.position = 'gk' then 1 else 0
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
		pms.nb_cards_second_yellow as away_cards_yellow_red,

		0 as home_win,
		case
			when ts.score > ts_home.score then 1 else 0
		end as away_win,

		0 as home_draw,
		case
			when ts.score = ts_home.score then 1 else 0
		end as away_draw,

		0 as home_lose,
		case
			when ts.score < ts_home.score then 1 else 0
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
	join (select * from dwh_{season}.player_main_stats where not played_home) as pms
	on pms.match = a.id
	join (select match, team, captain, score from dwh_{season}.team_stats where not played_home) as ts 
	on a.id = ts.match and ts.team = a.away_team
	join (select match, team, captain, score from dwh_{season}.team_stats where played_home) as ts_home
	on a.id = ts_home.match
	join (select * from dwh_{season}.compo where not played_home) c
	on a.id = c.match and pms.player = c.player
	left join (select match, team, player_in, player_out from dwh_{season}.event e join dwh_{season}.sub_event se on e.id = se.id where not e.played_home) as e
	on a.id = e.match and (e.player_in = c.player or e.player_out = c.player)
)
insert into tmp_players_ranking
select *
from home_stats
union all
select *
from away_stats