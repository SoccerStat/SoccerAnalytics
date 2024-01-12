/* TODO
 * (plus tard) Nombre de points rapportés à l'équipe en marquant
 * 
 * */
drop function if exists players_rankings;

create or replace function players_rankings(
	in id_chp varchar(100),
	in id_season varchar(20),
	/*in which varchar(20) default 'scorer',*/
	in first_week int default 1,
	in last_week int default 100,
	in side ranking_type default 'both'
)
returns table(
	--"Ranking" bigint,
	"Player" varchar(100),
	"Age" bigint,
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
	"xG Assists" numeric,
	"xG Assists /90" numeric,
	"Clean Sheets" bigint,
	"Yellow Cards" bigint,
	"Red Cards" bigint,
	"Incl. 2 Yellow Cards" bigint,
	"Minutes" bigint,
	"Captain" bigint,
	"Last Opponent" varchar(100)
	--Attendance numeric,
)
as $$
begin
	/*if which not in ('scorer', 'assist') then
		raise exception 'Invalid value for the type of player ranking. Valid values for "which" parameter are scorer, assist.';
	end if;*/
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

	/*with ranked_positions as (
		SELECT id_player, "position", ROW_NUMBER() OVER (PARTITION BY id_player ORDER BY COUNT(*) DESC) AS "position_rank"
		FROM player_stats
		GROUP BY id_player, "position"
	)*/

	with players_stats as (
		select
			p.name as Player,

			EXTRACT(YEAR FROM age(current_date, date_birth))::bigint/* || ' ans, ' ||
			EXTRACT(MONTH FROM age(current_date, date_birth)) || ' months, ' ||
			EXTRACT(DAY FROM age(current_date, date_birth)) || ' days'*/ AS Age,

			case
				when set_bigint_stat(sum(home_gk), sum(away_gk), side) > 0 then true
				else false
			end as GK,
			
			string_agg(distinct c.complete_name, ', ') as Club,

			set_bigint_stat(sum(home_nb_wins + home_nb_draws + home_nb_loses), sum(away_nb_wins + away_nb_draws + away_nb_loses), side) as Matches,

			set_bigint_stat(sum(home_nb_wins), sum(away_nb_wins), side) as Wins,
			set_bigint_stat(sum(home_nb_draws), sum(away_nb_draws), side) as Draws,
			set_bigint_stat(sum(home_nb_loses), sum(away_nb_loses), side) as Loses,
			
			set_bigint_stat(sum(home_goals), sum(away_goals), side) as Goals,
			set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) as Penalties,
			set_bigint_stat(sum(home_assists), sum(away_assists), side) as Assists,

			set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as xG,

			set_numeric_stat(sum(home_xg_assists)::numeric, sum(away_xg_assists)::numeric, side) as "xG Assists",

			set_bigint_stat(sum(home_clean_sheets), sum(away_clean_sheets), side) as "Clean Sheets",
			
			set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), side) as "Yellow Cards",
			set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side) as "Red Cards",
			set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side) as "Incl. 2 Yellow Cards",
			
			set_bigint_stat(sum(home_minutes), sum(away_minutes), side) as Minutes,

			set_bigint_stat(sum(home_captain), sum(away_captain), side) as Captain,
			
			get_last_opponent(c.id, id_season) as "Last Opponent"

		from
			(select
				ps.id_player,
				home_team as team,

				case
					when ps.position = 'gk' then 1
					else 0
				end as home_gk,
				0 as away_gk,
				
				ps.goals as home_goals,
				0 as away_goals,
				ps.pens_made as home_pens_made,
				0 as away_pens_made,
				ps.assists as home_assists,
				0 as away_assists,
				ps.xg as home_xg,
				0.0 as away_xg,
				ps.xg_assist as home_xg_assists,
				0.0 as away_xg_assists,

				case
					when h.away_score = 0 then 1
					else 0
				end as home_clean_sheets,
				0 as away_clean_sheets,

				ps.cards_yellow as home_cards_yellow,
				0 as away_cards_yellow,
				ps.cards_red + ps.cards_yellow_red as home_cards_red,
				0 as away_cards_red,
				ps.cards_yellow_red as home_cards_yellow_red,
				0 as away_cards_yellow_red,

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
				0 as away_nb_loses,

				case
					when ts.id_captain = ps.id_player then 1 else 0
				end as home_captain,
				0 as away_captain,

				ps.minutes as home_minutes,
				0 as away_minutes
			from player_stats ps
			left join match h
			on ps.id_match = h.id
			left join team_stats ts 
			on h.home_team = ts.id_team and h.id = ts.id_match
			--left join team_stats ts2
			--on h.id = ts2.id_match and ts.id_team <> ts2.id_team
			where 
				ps.played_home and
				h.id_championship = id_chp and
				h.season = id_season and
							
				length(week) <= 2 and
				
				cast(week as int) >= first_week and
				cast(week as int) <= last_week
			
			union all

			select
				ps.id_player,
				away_team as team,

				0 as home_gk,
				case
					when ps.position = 'gk' then 1
					else 0
				end as away_gk,
				
				0 as home_goals,
				ps.goals as away_goals,
				0 as home_pens_made,
				ps.pens_made as away_pens_made,
				0 as home_assists,
				ps.assists as away_assists,
				0.0 as home_xg,
				ps.xg as away_xg,
				0.0 as home_xg_assists,
				ps.xg_assist as away_xg_assists,

				0 as home_clean_sheets,
				case
					when a.home_score = 0 then 1
					else 0
				end as away_clean_sheets,

				0 as home_cards_yellow,
				ps.cards_yellow as away_cards_yellow,
				0 as home_cards_red,
				ps.cards_red + ps.cards_yellow_red as away_cards_red,
				0 as home_cards_yellow_red,
				ps.cards_yellow_red as away_cards_yellow_red,

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
				end as away_nb_loses,

				0 as home_captain,
				case
					when ts.id_captain = ps.id_player then 1 else 0
				end as away_captain,

				0 as home_minutes,
				ps.minutes as away_minutes
			from player_stats ps
			left join match a
			on ps.id_match = a.id
			left join team_stats ts 
			on a.away_team = ts.id_team and a.id = ts.id_match
			--left join team_stats ts2
			--on a.id = ts2.id_match and ts.id_team <> ts2.id_team
			where 
				not ps.played_home and
				a.id_championship = id_chp and
				a.season = id_season and
				
				length(week) <= 2 and
				
				cast(week as int) >= first_week and
				cast(week as int) <= last_week
			) as "stats"
			join club c 
			on team = c.id
			join player p
			on stats.id_player = p.id
		group by Player, Age, "Last Opponent"
	)

	select 
		/*rank() over (
			order by
				ps.Goals desc, 
				ps.Penalties asc, 
				ps.Assists desc, 
				ps.Minutes asc
		) as Ranking,*/
		
		ps.Player,
		ps.Age,
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
			when ps.Matches <> 0 then
				round(ps.xG / ps.Matches, 2)
			else 0.0
		end as "xG/90",

		ps."xG Assists",
		case
			when ps.Matches <> 0 then
				round(ps."xG Assists" / ps.Matches, 2)
			else 0.0
		end as "xG Assists /90",

		ps."Clean Sheets",

		ps."Yellow Cards",
		ps."Red Cards",
		ps."Incl. 2 Yellow Cards",

		ps.Minutes,

		ps.Captain,

		ps."Last Opponent"
	from players_stats ps;
end;
$$ language plpgsql;
