drop function if exists teams_ranking;

create or replace function teams_ranking(
	in id_chp varchar(100),
	in id_season varchar(20),
	in first_week int default 1,
	in last_week int default 100,
	in side ranking_type default 'both'
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
	"Shots on Target" bigint,
	"Last Opponent" varchar(100)
)
as $$
begin
	if id_chp not in (select id from championship) then
		raise exception 'Invalid value for id_chp. Valid values are ligue_1, premier_league, serie_a, la_liga, fussball_bundesliga';
	end if;
	if id_season  !~ '^\d{4}-(\d{4})$' or (substring(id_season, 1, 4)::int + 1)::text != substring(id_season, 6, 4) then 
		raise exception 'Wrong format of season. It should be like "2022-2023".';
	end if;
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week';
	end if;
	if side not in ('home', 'away', 'both') then
        raise exception 'Invalid value for ranking_type. Valid values are: home, away, both';
    end if;
   
	return query

	with selected_match as (
		select id, home_team, away_team, attendance, home_score, away_score
		from match 
		where 
			id_championship = id_chp and 
			season = id_season and 
			length(week) <= 2 and 
			cast(week as int) >= first_week and 
			cast(week as int) <= last_week
	),
	teams_stats as (
		select
			complete_name as Club,
			
			round(avg(att), 0) as Attendance,
			
			set_bigint_stat(sum(case when home_match then 1 else 0 end), sum(case when away_match then 1 else 0 end), side) as Matches,
			
			set_bigint_stat(sum(home_points), sum(away_points), side) as Points,
			
			set_bigint_stat(sum(case when home_win then 1 else 0 end), sum(case when away_win then 1 else 0 end), side) as Wins,
			set_bigint_stat(sum(case when home_draw then 1 else 0 end), sum(case when away_draw then 1 else 0 end), side) as Draws,
			set_bigint_stat(sum(case when home_lose then 1 else 0 end), sum(case when away_lose then 1 else 0 end), side) as Loses,
			
			set_bigint_stat(sum(home_goal_for), sum(away_goal_for), side) as "Goals For",
			set_bigint_stat(sum(home_goal_against), sum(away_goal_against), side) as "Goals Against",
			set_bigint_stat(sum(home_goal_for - home_goal_against), sum(away_goal_for - away_goal_against), side) as "Goals Diff",

			set_bigint_stat(sum(case when home_clean_sheet then 1 else 0 end), sum(case when away_clean_sheet then 1 else 0 end), side) as "Clean Sheets",
			
			set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side) as "xG For",

			set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side) as "xG Against",

			set_bigint_stat(sum(home_y_cards), sum(away_y_card), side) as "Yellow Cards",
			set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side) as "Red Cards",
			set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side) as "Incl. 2 Yellow Cards",
			
			set_bigint_stat(sum(home_fouls), sum(away_fouls), side) as Fouls,

			set_bigint_stat(sum(home_shots), sum(away_shots), side) as Shots,
			set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), side) as "Shots on Target",

			get_last_opponent(c.id, id_season) as "Last Opponent"
			
		from
			(select
				true as home_match,
				false as away_match,

				home_team as team,
				h.attendance as att,
				
				ts.y_cards as home_y_cards,
				0 as away_y_card,
				ts.yr_cards as home_yr_cards,
				0 as away_yr_cards,
				ts.r_cards as home_r_cards,
				0 as away_r_cards,
				
				ts.fouls as home_fouls,
				0 as away_fouls,

				ts.shots as home_shots,
				0 as away_shots,

				ts.on_target as home_shots_ot,
				0 as away_shots_ot,
				
				home_score as home_goal_for,
				0 as away_goal_for,
				away_score as home_goal_against,
				0 as away_goal_against,

				case
					when away_score = 0 then true else false
				end as home_clean_sheet,
				false as away_clean_sheet,

				ts.xg as home_xg_for,
				0.0 as away_xg_for,

				--(select xg from team_stats ts_away where ts_away.id_team = h.away_team and ts_away.id_match = h.id) as home_xg_against,
				ts_away.xg as home_xg_against,
				0.0 as away_xg_against,

				case
					when home_score > away_score then 3
					when home_score = away_score then 1
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
					when home_score > away_score then true else false
				end as home_win,
				false as away_win,

				case
					when home_score = away_score then true else false
				end as home_draw,
				false as away_draw,

				case
					when home_score < away_score then true else false
				end as home_lose,
				false as away_lose
			from selected_match as h
			left join team_stats ts 
			on h.id = ts.id_match and h.home_team = ts.id_team
			left join team_stats ts_away
			on h.id = ts_away.id_match and h.away_team = ts_away.id_team
			union all
			
			select
				false as home_match,
				true as away_match,

				away_team as team,
				null as att,
				
				0 as home_y_card,
				ts.y_cards as away_y_cards,
				0 as home_yr_cards,
				ts.yr_cards as away_yr_cards,
				0 as home_r_cards,
				ts.r_cards as away_r_cards,
				
				0 as home_fouls,
				ts.fouls as away_fouls,

				0 as home_shots,
				ts.shots as away_shots,

				0 as home_shots_ot,
				ts.on_target as away_shots_ot,

				0 as home_goal_for,
				away_score as away_goal_for,
				0 as home_goal_against,
				home_score as away_goal_against,

				false as home_clean_sheet,
				case
					when home_score = 0 then true else false
				end as away_clean_sheet,

				0.0 as home_xg_for,
				ts.xg as away_xg_for,

				0.0 as home_xg_against,
				--(select xg from team_stats ts_home where ts_home.id_team = a.home_team and ts_home.id_match = a.id) as away_xg_against,
				ts_home.xg as away_xg_against,

				0 as home_points,
				case
					when away_score > home_score then 3
					when away_score = home_score then 1
					else 0
				end as away_points,

				0 as home_x_points,
				case
					when ts.xg > ts_home.xg then 3
					when ts.xg = ts_home.xg then 1
					else 0
				end as away_x_points,
				
				false as home_win,
				case
					when away_score > home_score then true else false
				end as away_win,

				false as home_draw,
				case
					when away_score = home_score then true else false
				end as away_draw,

				false as home_lose,
				case
					when away_score < home_score then true else false
				end as away_lose
			from selected_match as a
			left join team_stats ts 
			on a.away_team = ts.id_team and a.id = ts.id_match
			left join team_stats ts_home
			on a.id = ts_home.id_match and a.home_team = ts_home.id_team) as "stats"
		join (select id, complete_name from club) as c
		on team = c.id
		group by Club, "Last Opponent"
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
		ts."Shots on Target",

		ts."Last Opponent"
	from teams_stats ts;
		
end;
$$ language plpgsql;
