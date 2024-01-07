/* TODO
 * (plus tard) Nombre de points rapportés à l'équipe en marquant
 * 
 * */
drop function if exists players_rankings;

create or replace function players_rankings(
	in id_chp varchar(100),
	in id_season varchar(20),
	in which varchar(20) default 'scorer',
	in first_week int default 1,
	in last_week int default 100,
	in side ranking_type default 'both'
)
returns table(
	"Ranking" bigint,
	"Player" varchar(100),
	"GK" bool,
	"Club" text,
	"Matches" bigint,
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
	"Last Opponent" varchar(100)
	--Attendance numeric,
)
as $$
begin
	if which not in ('scorer', 'assist') then
		raise exception 'Invalid value for the type of player ranking. Valid values for "which" parameter are scorer, assist.';
	end if;
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

	select
		rank() over (order by set_bigint_stat(sum(home_goals), sum(away_goals), side) desc, set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) asc, set_bigint_stat(sum(home_assists), sum(away_assists), side) desc, set_bigint_stat(sum(home_minutes), sum(away_minutes), side) asc) as Ranking,
		
		p.name as Player,

		case
			when set_bigint_stat(sum(home_gk), sum(away_gk), side) > 0 then true
			else false
		end as "GK",
		
		/*case
			when ps.position = 'gk' then true
			else false
		end as "GK",*/
		
		string_agg(distinct c.complete_name, ', ') as Club,

		set_bigint_stat(sum(home_matches), sum(away_matches), side) as Matches,
		
		set_bigint_stat(sum(home_goals), sum(away_goals), side) as Goals,
		set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) as Penalties,
		set_bigint_stat(sum(home_assists), sum(away_assists), side) as Assists,

		set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as "xG",
		round(set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) / set_bigint_stat(sum(home_matches), sum(away_matches), side)::numeric, 2) as "xG/90",

		set_numeric_stat(sum(home_xg_assists)::numeric, sum(away_xg_assists)::numeric, side) as "xG Assists",
		round(set_numeric_stat(sum(home_xg_assists)::numeric, sum(away_xg_assists)::numeric, side) / set_bigint_stat(sum(home_matches), sum(away_matches), side)::numeric, 2) as "xG Assists /90",

		set_bigint_stat(sum(home_clean_sheets), sum(away_clean_sheets), side) as "Clean Sheets",
		
		set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), side) as "Yellow Cards",
		set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side) as "Red Cards",
		set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side) as "Incl. 2 Yellow Cards",
		
		set_bigint_stat(sum(home_minutes), sum(away_minutes), side) as "Minutes",
		
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
				when h.away_score = 0 then 1 /*where clause: played_home already = true*/
				else 0
			end as home_clean_sheets,
			0 as away_clean_sheets,

			ps.cards_yellow as home_cards_yellow,
			0 as away_cards_yellow,
			ps.cards_red + ps.cards_yellow_red as home_cards_red,
			0 as away_cards_red,
			ps.cards_yellow_red as home_cards_yellow_red,
			0 as away_cards_yellow_red,

			1 as home_matches,
			0 as away_matches,

			ps.minutes as home_minutes,
			0 as away_minutes
		from player_stats ps
		left join match h
		on ps.id_match = h.id
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
				when a.home_score = 0 then 1 /*where clause: played_home already = false*/
				else 0
			end as away_clean_sheets,

			0 as home_cards_yellow,
			ps.cards_yellow as away_cards_yellow,
			0 as home_cards_red,
			ps.cards_red + ps.cards_yellow_red as away_cards_red,
			0 as home_cards_yellow_red,
			ps.cards_yellow_red as away_cards_yellow_red,

			0 as home_matches,
			1 as away_matches,

			0 as home_minutes,
			ps.minutes as away_minutes
		from player_stats ps
		left join match a
		on ps.id_match = a.id
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
	group by Player, "Last Opponent";
end;
$$ language plpgsql;
