drop function if exists public.teams_ranking;

create or replace function public.teams_ranking(
	in id_chp varchar(100),
	in id_season varchar(20),
	in first_week int,
	in last_week int,
	in side ranking_type
)
returns table(
	"Ranking" bigint,
	"Club" varchar(100),
	"Attendance" numeric,
	"Matches" bigint,
	"Points" bigint,
	"Points/Match" numeric,
	"Wins" bigint,
	"Draws" bigint,
	"Loses" bigint,
	"Goals For" bigint,
	"Goals Against" bigint,
	"Goals Diff" bigint,
	"Clean Sheets" bigint,
	"xG For" numeric,
	"xG For /Match" numeric,
	"xG Against" numeric,
	"xG Against /Match" numeric,
	"Yellow Cards" bigint,
	"Red Cards" bigint,
	"Incl. 2 Yellow Cards" bigint,
	"Fouls" bigint,
	"Shots" bigint,
	"Shots on Target" bigint--,
	--"Last Opponent" varchar(100)
)
as $$
DECLARE
    season_schema text;
	query text;
begin
	PERFORM check_parameters(id_chp, id_season, first_week, last_week, side);

	season_schema = 'dwh_' || id_season;
    ----,--get_last_opponent(c.id, id_season) as "Last Opponent"
	query := format(
		'with selected_match as (
			select id, home_team, away_team, attendance, competition
			from %I.match 
			where 
				competition = ''' || id_chp || ''' and 
				length(week) <= 2 and 
				cast(week as int) between ''' || first_week || ''' and ''' || last_week || '''
		),
		home_team as (
			select
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
			left join %I.team_stats ts 
			on h.id = ts.match and h.home_team = ts.team
			left join %I.team_stats ts_away
			on h.id = ts_away.match and h.away_team = ts_away.team
		),
		away_team as (
			select
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
			left join %I.team_stats ts 
			on a.away_team = ts.team and a.id = ts.match
			left join %I.team_stats ts_home
			on a.id = ts_home.match and a.home_team = ts_home.team
		),
		teams_stats as (
			select
				c.name as Club,
				
				round(avg(att), 0) as Attendance,

				sum(home_score) as home_score,
				sum(away_score) as away_score,
				
				set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,
				
				set_bigint_stat(sum(home_points), sum(away_points), ''' || side || ''') as Points,
				
				set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
				set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
				set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,
				
				set_bigint_stat(sum(home_goal_for), sum(away_goal_for), ''' || side || ''') as "Goals For",
				set_bigint_stat(sum(home_goal_against), sum(away_goal_against), ''' || side || ''') as "Goals Against",
				set_bigint_stat(sum(home_goal_for - home_goal_against), sum(away_goal_for - away_goal_against), ''' || side || ''') as "Goals Diff",

				set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), ''' || side || ''') as "Clean Sheets",
				
				set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, ''' || side || ''') as "xG For",

				set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, ''' || side || ''') as "xG Against",

				set_bigint_stat(sum(home_y_cards), sum(away_y_card), ''' || side || ''') as "Yellow Cards",
				set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards",
				set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
				
				set_bigint_stat(sum(home_fouls), sum(away_fouls), ''' || side || ''') as Fouls,

				set_bigint_stat(sum(home_shots), sum(away_shots), ''' || side || ''') as Shots,
				set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), ''' || side || ''') as "Shots on Target"
				
			from (
				select * 
				from home_team
				union all
				select *
				from away_team
			) as "stats"
			join (select id, name from dwh_upper.club) as c
			on team = competition || ''_'' || c.id
			group by Club--, "Last Opponent"
		)

		select
			rank() over (
					order by 
						ts.Points desc, 
						ts."Goals Diff" desc
			) as Ranking,

			ts.Club,

			ts.Attendance,
			ts.Matches,

			ts.Points,
			round(ts.Points / ts.Matches::numeric, 2) as "Points/Match",

			ts.Wins,
			ts.Draws,
			ts.Loses,

			ts."Goals For",
			ts."Goals Against",
			ts."Goals Diff",

			ts."Clean Sheets",

			ts."xG For",
			case
				when ts.Matches <> 0 then
					round(ts."xG For" / ts.Matches::numeric, 2)
				else 0.0 
			end as "xG For /Match",

			ts."xG Against",
			case
				when ts.Matches <> 0 then
					round(ts."xG Against" / ts.Matches::numeric, 2)
				else 0.0
			end as "xG Against /Match",

			ts."Yellow Cards",
			ts."Red Cards",
			ts."Incl. 2 Yellow Cards",
			ts.Fouls,

			ts.Shots,
			ts."Shots on Target"--,

			--ts."Last Opponent"
		from teams_stats ts;',
		season_schema, season_schema, season_schema, season_schema, season_schema
	);

	RETURN QUERY EXECUTE query USING id_chp, id_season, first_week, last_week, side;
		
end;
$$ language plpgsql;
