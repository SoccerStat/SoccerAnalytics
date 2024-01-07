/* TODO
 * ranking : ajouter paramètre pour savoir quel classement on veut établir (sur les cartons, les buts marqués / encaissés etc..
 * penalties */
drop function if exists teams_rankings;

create or replace function teams_rankings(
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
	"xG Diff" numeric,
	/*"xG Diff /Match" numeric,*/
	"Yellow Cards" bigint,
	"Red Cards" bigint,
	"Incl. 2 Yellow Cards" bigint,
	"Fouls" bigint,
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
	select
		rank() over (order by set_bigint_stat(sum(home_points), sum(away_points), side) desc, set_bigint_stat(sum(home_goal_for - home_goal_against), sum(away_goal_for - away_goal_against), side) desc) as Ranking,
		
		complete_name as Club,
		
		round(avg(att), 0) as Attendance,
		
		set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side) as Matches,
		
		set_bigint_stat(sum(home_points), sum(away_points), side) as Points,
		round(set_bigint_stat(sum(home_points), sum(away_points), side)::numeric / set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side)::numeric, 2) as "Points/Match",
		
		set_bigint_stat(sum(home_nb_wins), sum(away_nb_wins), side) as Wins,
		set_bigint_stat(sum(home_nb_draws), sum(away_nb_draws), side) as Draws,
		set_bigint_stat(sum(home_nb_loses), sum(away_nb_loses), side) as Loses,
		
		set_bigint_stat(sum(home_goal_for), sum(away_goal_for), side) as "Goals For",
		set_bigint_stat(sum(home_goal_against), sum(away_goal_against), side) as "Goals Against",
		set_bigint_stat(sum(home_goal_for - home_goal_against), sum(away_goal_for - away_goal_against), side) as "Goals Diff",

		set_bigint_stat(sum(home_clean_sheets), sum(away_clean_sheets), side) as "Clean Sheets",
		
		set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side) as "xG For",
		round(set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side) / set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side)::numeric, 2) as "xG For /Match",

		set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side) as "xG Against",
		round(set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side) / set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side)::numeric, 2) as "xG Against /Match",

		set_numeric_stat(sum(home_xg_for - home_xg_against)::numeric, sum(away_xg_for - away_xg_against)::numeric, side) as "xG Diff",
		/*round(set_numeric_stat(cast(sum(home_xg_for - home_xg_against) as numeric), cast(sum(away_xg_for - away_xg_against) as numeric), side) / set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side)::numeric, 2) as "xG Diff /Match",*/
		
		set_bigint_stat(sum(home_y_cards), sum(away_y_card), side) as "Yellow Cards",
		set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side) as "Red Cards",
		set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side) as "Incl. 2 Yellow Cards",
		
		set_bigint_stat(sum(home_fouls), sum(away_fouls), side) as "Fouls",

		get_last_opponent(c.id, id_season) as "Last Opponent"
		
	from
		(select
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
			
			home_score as home_goal_for,
			0 as away_goal_for,
			away_score as home_goal_against,
			0 as away_goal_against,

			case
				when away_score = 0 then 1
				else 0
			end as home_clean_sheets,
			0 as away_clean_sheets,

			ts.xg as home_xg_for,
			0.0 as away_xg_for,

			(select xg from team_stats ts_away where ts_away.id_team = h.away_team and ts_away.id_match = h.id) as home_xg_against,
			0.0 as away_xg_against,

			case
				when home_score > away_score then 3
				when home_score = away_score then 1
				else 0
			end as home_points,
			0 as away_points,
			case
				when home_score > away_score then 1 else 0
			end as home_nb_wins,
			0 as away_nb_wins,
			case
				when home_score = away_score then 1 else 0
			end as home_nb_draws,
			0 as away_nb_draws,
			case
				when home_score < away_score then 1 else 0
			end as home_nb_loses,
			0 as away_nb_loses
		from match h
		left join team_stats ts 
		on h.home_team = ts.id_team and h.id = ts.id_match
		where 
			id_championship = id_chp and 
			season = id_season and 
			length(week) <= 2 and 
			cast(week as int) >= first_week and 
			cast(week as int) <= last_week
		
		union all
		
		select
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

			0 as home_goal_for,
			away_score as away_goal_for,
			0 as home_goal_against,
			home_score as away_goal_against,

			0 as home_clean_sheets,
			case
				when home_score = 0 then 1
				else 0
			end as away_clean_sheets,

			0.0 as home_xg_for,
			ts.xg as away_xg_for,

			0.0 as home_xg_against,
			(select xg from team_stats ts_home where ts_home.id_team = a.home_team and ts_home.id_match = a.id) as away_xg_against,

			0 as home_points,
			case
				when away_score > home_score then 3
				when away_score = home_score then 1
				else 0
			end as away_points,
			
			0 as home_nb_wins,
			case
				when away_score > home_score then 1 else 0
			end as away_nb_wins,
			0 as home_nb_draws,
			case
				when away_score = home_score then 1 else 0
			end as away_nb_draws,
			0 as home_nb_loses,
			case
				when away_score < home_score then 1 else 0
			end as away_nb_loses
		from match a
		left join team_stats ts 
		on a.away_team = ts.id_team and a.id = ts.id_match
		where 
			id_championship = id_chp and 
			season = id_season and 
			
			length(week) <= 2 and 
			cast(week as int) >= first_week and 
			cast( week as int) <= last_week) as "stats"
	join club c
	on team = c.id
	group by Club, "Last Opponent";
end;
$$ language plpgsql;
