with selected_matches as materialized (
	select
		m.id,
		m.home_team,
		m.away_team,
		m.date,
		m.time,
		m.week,
		m.round,
		m.extra_time,
		m.leg,
		m.attendance,
		m.competition as id_comp,
		coalesce(chp.name, c_cup.name) as competition
	from season_{season}.match m
	left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
	where m.competition = '{id_comp}'
	and (
	    notes is null
	    or lower(notes) not like '%match cancelled%'
	)
),
home_team as (
	select
		'{season}' as season,
		h.id_comp,
		h.competition,

		h.id as id_match,
		home_team as id_team,
		c1.name as club,
		away_team as id_opponent,
		c2.name as opponent,
		true as played_home,

		ts.manager as home_manager,
		ts_away.manager as away_manager,

		1 as home_match,
		0 as away_match,
		ts.score as home_score,
		ts_away.score as away_score,
		case
		    when h.extra_time
		    then 120
		    else 90
		end as home_minutes,
		0 as away_minutes,

		ts.penalty_shootout_scored as home_penalty_shootout_scored,
		ts_away.penalty_shootout_scored as away_penalty_shootout_scored,
		ts.penalty_shootout_total as home_penalty_shootout_total,
		ts_away.penalty_shootout_total as away_penalty_shootout_total,

		h.date,
		h.time,
		h.week,
		h.round,
		h.leg,
		h.extra_time,
		h.attendance as att,

		case
			when ts.penalty_shootout_scored is null and ts.score > ts_away.score
			then 1
			when ts.penalty_shootout_scored is not null and ts.penalty_shootout_scored > ts_away.penalty_shootout_scored
			then 1
			else 0
		end as home_win,
		0 as away_win,
		case
			when ts.penalty_shootout_scored is null and ts.score = ts_away.score
			then 1
			else 0
		end as home_draw,
		0 as away_draw,
		case
			when ts.penalty_shootout_scored is null and ts.score < ts_away.score
			then 1
			when ts.penalty_shootout_scored is not null and ts.penalty_shootout_scored < ts_away.penalty_shootout_scored
			then 1
			else 0
		end as home_lose,
		0 as away_lose,
		
		ts.score as home_goals_for,
		0 as away_goals_for,
		ts_away.score as home_goals_against,
		0 as away_goals_against,

		ts.nb_shots_total as home_shots_for,
		0 as away_shots_for,
		ts.nb_shots_on_target as home_shots_ot_for,
		0 as away_shots_ot_for,

		ts_away.nb_shots_total as home_shots_against,
		0 as away_shots_against,
		ts_away.nb_shots_on_target as home_shots_ot_against,
		0 as away_shots_ot_against,

		case
			when ts_away.score = 0 then 1 else 0
		end as home_clean_sheet,
		0 as away_clean_sheet,

		ts.nb_cards_yellow as home_y_cards,
		0 as away_y_cards,
		ts.nb_cards_second_yellow as home_yr_cards,
		0 as away_yr_cards,
		ts.nb_cards_red as home_r_cards,
		0 as away_r_cards,
		
		ts.nb_fouls as home_fouls,
		0 as away_fouls,

		ts.xg as home_xg_for,
		0.0 as away_xg_for,
		ts_away.xg as home_xg_against,
		0.0 as away_xg_against,

		case
			when ts.score > ts_away.score then 3
			when ts.score = ts_away.score then 1
			else 0
		end as home_points,
		0 as away_points,
		case
			when ts.xg > ts_away.xg then 3
			when ts.xg = ts_away.xg then 1
			else 0
		end as home_x_points,
		0 as away_x_points,

		ts.nb_passes_succ as home_passes_succ,
		0 as away_passes_succ,

		ts.nb_passes_total as home_passes_total,
		0 as away_passes_total
	from selected_matches as h
	left join season_{season}.team_stats ts 
	on h.id = ts.match and h.home_team = ts.team
	left join season_{season}.team_stats ts_away
	on h.id = ts_away.match and h.away_team = ts_away.team
	left join (select id, name from upper.club) c1
	on h.home_team = h.id_comp || '_' || c1.id
	left join (select id, name from upper.club) c2
	on h.away_team = h.id_comp || '_' || c2.id
)
insert into analytics.staging_teams_performance
select *
from home_team;


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
		m.extra_time,
		m.attendance,
		m.competition as id_comp,
		coalesce(chp.name, c_cup.name) as competition
	from season_{season}.match m
	left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
	where m.competition = '{id_comp}'
	and (
	    notes is null
	    or lower(notes) not like '%match cancelled%'
	)
),
away_team as (
	select
		'{season}' as season,
		a.id_comp,
		a.competition,

		a.id as id_match,
		away_team as id_team,
		c1.name as club,
		home_team as id_opponent,
		c2.name as opponent,
		false as played_home,

		ts_home.manager as home_manager,
		ts.manager as away_manager,

		0 as home_match,
		1 as away_match,
		ts_home.score as home_score,
		ts.score as away_score,
		0 as home_minutes,
		case
		    when a.extra_time
		    then 120
		    else 90
		end as away_minutes,

		ts_home.penalty_shootout_scored as home_penalty_shootout_scored,
		ts.penalty_shootout_scored as away_penalty_shootout_scored,
		ts_home.penalty_shootout_total as home_penalty_shootout_total,
		ts.penalty_shootout_total as away_penalty_shootout_total,

		a.date,
		a.time,
		a.week,
		a.round,
		a.leg,
		a.extra_time,
		case when a.round = 'Final' then a.attendance else null::numeric end as att,

		0 as home_win,
		case
			when ts.penalty_shootout_scored is null and ts.score > ts_home.score
			then 1
			when ts.penalty_shootout_scored is not null and ts.penalty_shootout_scored > ts_home.penalty_shootout_scored
			then 1
			else 0
		end as away_win,
		0 as home_draw,
		case
			when ts.penalty_shootout_scored is null and ts.score = ts_home.score
			then 1
			else 0
		end as home_draw,
		0 as away_lose,
		case
			when ts.penalty_shootout_scored is null and ts.score < ts_home.score
			then 1
			when ts.penalty_shootout_scored is not null and ts.penalty_shootout_scored < ts_home.penalty_shootout_scored
			then 1
			else 0
		end as away_lose,

		0 as home_goals_for,
		ts.score as away_goals_for,
		0 as home_goals_against,
		ts_home.score as away_goals_against,

		0 as home_shots_for,
		ts.nb_shots_total as away_shots_for,
		0 as home_shots_ot_for,
		ts.nb_shots_on_target as away_shots_ot_for,

		0 as home_shots_against,
		ts_home.nb_shots_total as away_shots_against,
		0 as home_shots_ot_against,
		ts_home.nb_shots_on_target as away_shots_ot_against,

		0 as home_clean_sheet,
		case
			when ts_home.score = 0 then 1 else 0
		end as away_clean_sheet,

		0 as home_y_cards,
		ts.nb_cards_yellow as away_y_cards,
		0 as home_yr_cards,
		ts.nb_cards_second_yellow as away_yr_cards,
		0 as home_r_cards,
		ts.nb_cards_red as away_r_cards,
		
		0 as home_fouls,
		ts.nb_fouls as away_fouls,

		0.0 as home_xg_for,
		ts.xg as away_xg_for,
		0.0 as home_xg_against,
		ts_home.xg as away_xg_against,

		0 as home_points,
		case
			when ts.score > ts_home.score then 3
			when ts.score = ts_home.score then 1
			else 0
		end as away_points,
		0 as home_x_points,
		case
			when ts.xg > ts_home.xg then 3
			when ts.xg = ts_home.xg then 1
			else 0
		end as away_x_points,

		0 as home_passes_succ,
		ts.nb_passes_succ as away_passes_succ,

		0 as home_passes_total,
		ts.nb_passes_total as away_passes_total
	from selected_matches as a
	left join season_{season}.team_stats ts 
	on a.away_team = ts.team and a.id = ts.match
	left join season_{season}.team_stats ts_home
	on a.id = ts_home.match and a.home_team = ts_home.team
	left join (select id, name from upper.club) c1
	on a.away_team = a.id_comp || '_' || c1.id
	left join (select id, name from upper.club) c2
	on a.home_team = a.id_comp || '_' || c2.id
)
insert into analytics.staging_teams_performance
select *
from away_team;