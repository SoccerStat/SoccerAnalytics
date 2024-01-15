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
	"Height" bigint,
	"Weight" bigint,
	"Nationalities" varchar[],
	"Footed" varchar(20),
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
	"Started" bigint,
	"Sub In" bigint,
	"Sub Out" bigint,
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

	with selected_match as (
		select id, home_team, away_team, home_score, away_score 
		from match
		where 
			id_championship = id_chp and
			season = id_season and
			length(week) <= 2 and
			cast(week as int) >= first_week and
			cast(week as int) <= last_week
	),

	players_compo as (
		select
			m.id,
			c.id_player,
			c.starting,
			case 
				when not c.starting then true
				else false
			end as sub_in,
			case
				when e.id_player_out = c.id_player then true
				else false
			end as sub_out
		from compo c
		join (select id, id_championship, season from match where id_championship = id_chp and season = season) as m
		on c.id_match = m.id
		join player_stats ps
		on c.id_match = ps.id_match and ps.id_player = c.id_player
		left join event e
		on m.id = e.id_match and c.id_club = e.id_club and (c.id_player = e.id_player_out)
	),

	players_stats as (
		select
			p.name as Player,

			EXTRACT(YEAR FROM age(current_date, date_birth))::bigint/* || ' ans, ' ||
			EXTRACT(MONTH FROM age(current_date, date_birth)) || ' months, ' ||
			EXTRACT(DAY FROM age(current_date, date_birth)) || ' days'*/ AS Age,

			p.height::bigint as Height,
			p.weight::bigint as Weight,

			array_agg(distinct pn.id_nationality) as Nationalities,
			p.footed as Footed,

			case
				when set_bigint_stat(sum(case when home_gk then 1 else 0 end), sum(case when away_gk then 1 else 0 end), side) > 0 then true
				else false
			end as GK,
			
			string_agg(distinct c.complete_name, ', ') as Club,

			set_bigint_stat(sum(case when home_match then 1 else 0 end), sum(case when away_match then 1 else 0 end), side) as Matches,

			set_bigint_stat(sum(case when home_win then 1 else 0 end), sum(case when away_win then 1 else 0 end), side) as Wins,
			set_bigint_stat(sum(case when home_draw then 1 else 0 end), sum(case when away_draw then 1 else 0 end), side) as Draws,
			set_bigint_stat(sum(case when home_lose then 1 else 0 end), sum(case when away_lose then 1 else 0 end), side) as Loses,
			
			set_bigint_stat(sum(home_goals), sum(away_goals), side) as Goals,
			set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) as Penalties,
			set_bigint_stat(sum(home_assists), sum(away_assists), side) as Assists,

			set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as xG,

			set_numeric_stat(sum(home_xg_assists)::numeric, sum(away_xg_assists)::numeric, side) as "xG Assists",

			set_bigint_stat(sum(case when home_clean_sheet then 1 else 0 end), sum(case when away_clean_sheet then 1 else 0 end), side) as "Clean Sheets",
			
			set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), side) as "Yellow Cards",
			set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side) as "Red Cards",
			set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side) as "Incl. 2 Yellow Cards",
			
			set_bigint_stat(sum(home_minutes), sum(away_minutes), side) as Minutes,

			set_bigint_stat(sum(case when home_captain then 1 else 0 end), sum(case when away_captain then 1 else 0 end), side) as Captain,

			set_bigint_stat(sum(case when home_started then 1 else 0 end), sum(case when away_started then 1 else 0 end), side) as Started,
			set_bigint_stat(sum(case when home_sub_in then 1 else 0 end), sum(case when away_sub_in then 1 else 0 end), side) as "Sub In",
			set_bigint_stat(sum(case when home_sub_out then 1 else 0 end), sum(case when away_sub_out then 1 else 0 end), side) as "Sub Out",
			
			get_last_opponent(c.id, id_season) as "Last Opponent"

		from
			(select
				true as home_match,
				false as away_match,

				ps.id_player,
				home_team as team,

				case
					when ps.position = 'gk' then true else false
				end as home_gk,
				false as away_gk,
				
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
					when h.away_score = 0 then true else false
				end as home_clean_sheet,
				false as away_clean_sheet,

				ps.cards_yellow as home_cards_yellow,
				0 as away_cards_yellow,
				ps.cards_red + ps.cards_yellow_red as home_cards_red,
				0 as away_cards_red,
				ps.cards_yellow_red as home_cards_yellow_red,
				0 as away_cards_yellow_red,

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
				false as away_lose,

				case
					when ts.id_captain = ps.id_player then true else false
				end as home_captain,
				false as away_captain,

				ps.minutes as home_minutes,
				0 as away_minutes,

				c.starting as home_started,
				false as away_started,
				
				case
					when not c.starting then true else false
				end as home_sub_in,
				false as away_sub_in,
				
				case
					when e.id_player_out = c.id_player then true else false
				end as home_sub_out,
				false as away_sub_out
			from (select * from player_stats where played_home) as ps
			left join selected_match as h
			on ps.id_match = h.id
			left join (select id_match, id_team, id_captain from team_stats) as ts 
			on h.id = ts.id_match and h.home_team = ts.id_team
			left join compo c
			on h.id = c.id_match and h.home_team = c.id_club and ps.id_player = c.id_player
			left join (select id_match, id_club, id_player, id_player_out from event) as e
			on h.id = e.id_match and h.home_team = e.id_club and ps.id_player = e.id_player_out
			
			union all

			select
				false as home_match,
				true as away_match,

				ps.id_player,
				away_team as team,

				false as home_gk,
				case
					when ps.position = 'gk' then true else false
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

				false as home_clean_sheet,
				case
					when a.home_score = 0 then true else false
				end as away_clean_sheet,

				0 as home_cards_yellow,
				ps.cards_yellow as away_cards_yellow,
				0 as home_cards_red,
				ps.cards_red + ps.cards_yellow_red as away_cards_red,
				0 as home_cards_yellow_red,
				ps.cards_yellow_red as away_cards_yellow_red,

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
				end as away_lose,

				false as home_captain,
				case
					when ts.id_captain = ps.id_player then true else false
				end as away_captain,

				0 as home_minutes,
				ps.minutes as away_minutes,

				false as home_started,
				c.starting as away_started,
				
				false as home_sub_in,
				case
					when not c.starting then true else false
				end as away_sub_in,
				
				false as home_sub_out,
				case
					when e.id_player_out = c.id_player then true else false
				end as away_sub_out
			from (select * from player_stats where not played_home) as ps
			left join selected_match as a
			on ps.id_match = a.id
			left join (select id_match, id_team, id_captain from team_stats) as ts 
			on a.id = ts.id_match and a.away_team = ts.id_team
			left join (select id_match,id_club, id_player, starting from compo) c
			on a.id = c.id_match and a.away_team = c.id_club and ps.id_player = c.id_player
			left join (select id_match, id_club, id_player, id_player_out from event) as e
			on a.id = e.id_match and a.away_team = e.id_club and ps.id_player = e.id_player_out
			) as "stats"
			join (select id, complete_name from club) as c 
			on team = c.id
			join (select id, name, date_birth, footed, height, weight from player) as p
			on stats.id_player = p.id
			left join player_nationality pn
			on p.id = pn.id_player
		group by Player, Age, Height, Weight, Footed, "Last Opponent"
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

		ps.Height,
		ps.Weight,
		ps.Nationalities,

		ps.Footed,
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

		ps.Started,
		ps."Sub In",
		ps."Sub Out",

		ps."Last Opponent"
	from players_stats ps;
end;
$$ language plpgsql;
