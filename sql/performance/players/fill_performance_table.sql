with selected_matches as materialized (
	select 
		m.id,
		m.home_team,
		m.away_team,
		m.date,
		m.time,
		m.week,
		m.round,
		m.leg,
		m.competition as id_comp,
		coalesce(chp.name, c_cup.name) as competition
	from season_{season}.match m
	left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
	where m.competition = '{id_comp}'
),
home_stats as (
	select
		'{season}' as season,
		h.id_comp,
		h.competition,

		pms.player as id_player,
		h.home_team as id_team,
		h.away_team as id_opponent,

		1 as home_match,
		0 as away_match,

		h.date,
		h.time,
		h.week,
		h.round,
		h.leg,

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
			when pms.position = 'gk' then 1 
			else 0
		end as home_gk,
		0 as away_gk,
		
		pms.nb_goals as home_goals,
		0 as away_goals,
		pms.nb_assists as home_assists,
		0 as away_assists,

		pms.nb_pens_scored as home_pens_made,
		0 as away_pens_made,
		pms.nb_pens_att as home_pens_att,
		0 as away_pens_att,

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

	from selected_matches as h
	join (select * from season_{season}.player_main_stats where played_home) as pms
	on pms.match = h.id
	join (select match, team, captain, score from season_{season}.team_stats where played_home) as ts 
	on h.id = ts.match and ts.team = h.home_team
	join (select match, team, captain, score from season_{season}.team_stats where not played_home) as ts_away
	on h.id = ts_away.match
	join (select * from season_{season}.compo where played_home) as c
	on h.id = c.match and pms.player = c.player
	left join (select e.match, e.team, se.player_in, se.player_out from season_{season}.event e join season_{season}.sub_event se on (e.id = se.id and e.match = se.match) where e.played_home) as e
	on h.id = e.match and (e.player_in = c.player or e.player_out = c.player)
)
insert into analytics.staging_players_performance
select *
from home_stats;


with selected_matches as materialized (
	select 
		m.id,
		m.home_team,
		m.away_team,
		m.date,
		m.time,
		m.week,
		m.round,
		m.leg,
		m.competition as id_comp,
		coalesce(chp.name, c_cup.name) as competition
	from season_{season}.match m
	left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
	where m.competition = '{id_comp}'
),
away_stats as (
	select
		'{season}' as season,
		a.id_comp,
		a.competition,

		pms.player as id_player,
		a.away_team as id_team,
		a.home_team as id_opponent,

		0 as home_match,
		1 as away_match,

		a.date,
		a.time,
		a.week,
		a.round,
		a.leg,

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

		0 as home_gk,
		case
			when pms.position = 'gk' then 1 else 0
		end as away_gk,
		
		0 as home_goals,
		pms.nb_goals as away_goals,
		0 as home_assists,
		pms.nb_assists as away_assists,

		0 as home_pens_made,
		pms.nb_pens_scored as away_pens_made,

		0 as home_pens_att,
		pms.nb_pens_att as away_pens_att,

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

	from selected_matches as a
	join (select * from season_{season}.player_main_stats where not played_home) as pms
	on pms.match = a.id
	join (select match, team, captain, score from season_{season}.team_stats where not played_home) as ts 
	on a.id = ts.match and ts.team = a.away_team
	join (select match, team, captain, score from season_{season}.team_stats where played_home) as ts_home
	on a.id = ts_home.match
	join (select * from season_{season}.compo where not played_home) c
	on a.id = c.match and pms.player = c.player
	left join (select e.match, e.team, se.player_in, se.player_out from season_{season}.event e join season_{season}.sub_event se on (e.id = se.id and e.match = se.match) where not e.played_home) as e
	on a.id = e.match and (e.player_in = c.player or e.player_out = c.player)
)
insert into analytics.staging_players_performance
select *
from away_stats;