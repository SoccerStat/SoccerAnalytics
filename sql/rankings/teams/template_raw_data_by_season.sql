with selected_match as (
	select id, home_team, away_team, attendance, competition
	from season_{season}.match 
	where 
		competition = '{id_comp}' and 
		length(week) <= 2 and 
		cast(week as int) between '{first_week}' and '{last_week}'
),
home_team as (
	select
		'{season}' as season,
		1 as home_match,
		0 as away_match,

		h.competition,

		ts.score as home_score,
		0 as away_score,

		home_team as team,
		h.attendance as att,
		
		ts.nb_cards_yellow as home_y_cards,
		0 as away_y_card,
		ts.nb_cards_second_yellow as home_yr_cards,
		0 as away_yr_cards,
		ts.nb_cards_red as home_r_cards,
		0 as away_r_cards,
		
		ts.nb_fouls as home_fouls,
		0 as away_fouls,

		ts.nb_shots_total as home_shots,
		0 as away_shots,

		ts.nb_shots_on_target as home_shots_ot,
		0 as away_shots_ot,
		
		ts.score as home_goal_for,
		0 as away_goal_for,
		ts_away.score as home_goal_against,
		0 as away_goal_against,

		case
			when ts_away.score = 0 then 1 else 0
		end as home_clean_sheet,
		0 as away_clean_sheet,

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
		
		case
			when ts.score > ts_away.score then 1 else 0
		end as home_win,
		0 as away_win,

		case
			when ts.score = ts_away.score then 1 else 0
		end as home_draw,
		0 as away_draw,

		case
			when ts.score < ts_away.score then 1 else 0
		end as home_lose,
		0 as away_lose
	from selected_match as h
	left join season_{season}.team_stats ts 
	on h.id = ts.match and h.home_team = ts.team
	left join season_{season}.team_stats ts_away
	on h.id = ts_away.match and h.away_team = ts_away.team
),
away_team as (
	select
		'{season}' as season,
		0 as home_match,
		1 as away_match,

		a.competition,

		0 as home_score,
		ts.score as away_score,

		away_team as team,
		null::numeric as att,
		
		0 as home_y_card,
		ts.nb_cards_yellow as away_y_cards,
		0 as home_yr_cards,
		ts.nb_cards_second_yellow as away_yr_cards,
		0 as home_r_cards,
		ts.nb_cards_red as away_r_cards,
		
		0 as home_fouls,
		ts.nb_fouls as away_fouls,

		0 as home_shots,
		ts.nb_shots_total as away_shots,

		0 as home_shots_ot,
		ts.nb_shots_on_target as away_shots_ot,

		0 as home_goal_for,
		ts.score as away_goal_for,
		0 as home_goal_against,
		ts_home.score as away_goal_against,

		0 as home_clean_sheet,
		case
			when ts_home.score = 0 then 1 else 0
		end as away_clean_sheet,

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
		end as away_lose
	from selected_match as a
	left join season_{season}.team_stats ts 
	on a.away_team = ts.team and a.id = ts.match
	left join season_{season}.team_stats ts_home
	on a.id = ts_home.match and a.home_team = ts_home.team
)
insert into tmp_teams_ranking
select *
from home_team
union all
select *
from away_team;