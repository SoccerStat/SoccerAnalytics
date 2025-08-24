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
out_and_injured as (
    select
        e.match,
        e.team,
        se.player_out,

        MAX(
            case
                when player_in is not null
                    and player_out is not null
                    and lower(notes) not like '%injury'
                then 1
                else 0
            end
        ) as home_sub_out,

        MAX(
            case
                when player_in is null
                    and player_out is not null
                    and lower(notes) like '%injury'
                then 1
                else 0
            end
        ) as home_injured
    from season_{season}.event e
    join season_{season}.sub_event se
    on (e.id = se.id and e.match = se.match)
    where played_home
    group by match, team, player_out
),
home_stats as (
	select
		h.id_comp,
		h.competition,
		h.id as id_match,

		pms.player as id_player,
		h.home_team as id_team,
		h.away_team as id_opponent,

		c.number as home_number,
		null as away_number,

		string_to_array(pms.position, ',') as home_positions,
		array[]::varchar[] as away_positions,

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

		pms.nb_shots as home_shots,
		0 as away_shots,
		pms.nb_shots_on_target as home_shots_ot,
		0 as away_shots_ot,

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
		0 as away_sub_in

	from selected_matches as h
	join (select * from season_{season}.player_main_stats where played_home) as pms
	on pms.match = h.id
	join (select match, team, captain, score from season_{season}.team_stats where played_home) as ts 
	on h.id = ts.match and ts.team = h.home_team
	join (select match, team, captain, score from season_{season}.team_stats where not played_home) as ts_away
	on h.id = ts_away.match
	join (select * from season_{season}.compo where played_home) as c
	on h.id = c.match and pms.player = c.player
),
joined as (
    select
        '{season}' as season,
        h.id_comp,
        h.competition,
        h.id_match,
        h.id_player,
        h.id_team,

        MAX(h.id_opponent)           as id_opponent,
        MAX(h.home_number)           as home_number,
        MAX(h.away_number)           as away_number,
        MAX(h.home_positions)        as home_positions,
        MAX(h.away_positions)        as away_positions,
        MAX(h.home_match)            as home_match,
        MAX(h.away_match)            as away_match,
        MAX(h.date)                  as date,
        MAX(h.time)                  as time,
        MAX(h.week)                  as week,
        MAX(h.round)                 as round,
        MAX(h.leg)                   as leg,
        MAX(h.home_win)              as home_win,
        MAX(h.away_win)              as away_win,
        MAX(h.home_draw)             as home_draw,
        MAX(h.away_draw)             as away_draw,
        MAX(h.home_lose)             as home_lose,
        MAX(h.away_lose)             as away_lose,
        MAX(h.home_gk)               as home_gk,
        MAX(h.away_gk)               as away_gk,
        MAX(h.home_goals)            as home_goals,
        MAX(h.away_goals)            as away_goals,
        MAX(h.home_assists)          as home_assists,
        MAX(h.away_assists)          as away_assists,
        MAX(h.home_pens_made)        as home_pens_made,
        MAX(h.away_pens_made)        as away_pens_made,
        MAX(h.home_pens_att)         as home_pens_att,
        MAX(h.away_pens_att)         as away_pens_att,
        MAX(h.home_xg)               as home_xg,
        MAX(h.away_xg)               as away_xg,
        MAX(h.home_clean_sheet)      as home_clean_sheet,
        MAX(h.away_clean_sheet)      as away_clean_sheet,
        MAX(h.home_shots)            as home_shots,
        MAX(h.away_shots)            as away_shots,
        MAX(h.home_shots_ot)         as home_shots_ot,
        MAX(h.away_shots_ot)         as away_shots_ot,
        MAX(h.home_cards_yellow)     as home_cards_yellow,
        MAX(h.away_cards_yellow)     as away_cards_yellow,
        MAX(h.home_cards_red)        as home_cards_red,
        MAX(h.away_cards_red)        as away_cards_red,
        MAX(h.home_cards_yellow_red) as home_cards_yellow_red,
        MAX(h.away_cards_yellow_red) as away_cards_yellow_red,
        MAX(h.home_captain)          as home_captain,
        MAX(h.away_captain)          as away_captain,
        MAX(h.home_minutes)          as home_minutes,
        MAX(h.away_minutes)          as away_minutes,
        MAX(h.home_started)          as home_started,
        MAX(h.away_started)          as away_started,
        MAX(h.home_sub_in)           as home_sub_in,
        MAX(h.away_sub_in)           as away_sub_in,

        oi.home_sub_out,
        0 as away_sub_out,

        oi.home_injured,
        0 as away_injured
    from home_stats h
    left join out_and_injured oi
    on h.id_match = oi.match and h.id_team = oi.team and h.id_player = oi.player_out
    group by h.id_comp, h.competition, h.id_match, h.id_player, h.id_team
)
insert into analytics.staging_players_performance
select * from joined;


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
subs as (
    select
        e.match,
        e.team,
        se.player_out,

        MAX(
            case
                when player_in is not null
                    and player_out is not null
                    and lower(notes) not like '%injury'
                then 1
                else 0
            end
        ) as away_sub_out,

        MAX(
            case
                when player_in is null
                    and player_out is not null
                    and lower(notes) like '%injury'
                then 1
                else 0
            end
        ) as away_injured
    from season_{season}.event e
    join season_{season}.sub_event se
    on (e.id = se.id and e.match = se.match)
    where not played_home
    group by match, team, player_out
),
away_stats as (
	select
		a.id_comp,
		a.competition,
		a.id as id_match,

		pms.player as id_player,
		a.away_team as id_team,
		a.home_team as id_opponent,

		null as home_number,
        c.number as away_number,

		array[]::varchar[] as home_positions,
		string_to_array(pms.position, ',') as away_positions,

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

		0 as home_shots,
		pms.nb_shots as away_shots,
		0 as home_shots_ot,
		pms.nb_shots_on_target as away_shots_ot,

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
		end as away_sub_in

	from selected_matches as a
	join (select * from season_{season}.player_main_stats where not played_home) as pms
	on pms.match = a.id
	join (select match, team, captain, score from season_{season}.team_stats where not played_home) as ts 
	on a.id = ts.match and ts.team = a.away_team
	join (select match, team, captain, score from season_{season}.team_stats where played_home) as ts_home
	on a.id = ts_home.match
	join (select * from season_{season}.compo where not played_home) c
	on a.id = c.match and pms.player = c.player
),
joined as (
    select
        '{season}' as season,
        a.id_comp,
        a.competition,
        a.id_match,
        a.id_player,
        a.id_team,

        MAX(a.id_opponent)           as id_opponent,
        MAX(a.home_number)           as home_number,
        MAX(a.away_number)           as away_number,
        MAX(a.home_positions)        as home_positions,
        MAX(a.away_positions)        as away_positions,
        MAX(a.home_match)            as home_match,
        MAX(a.away_match)            as away_match,
        MAX(a.date)                  as date,
        MAX(a.time)                  as time,
        MAX(a.week)                  as week,
        MAX(a.round)                 as round,
        MAX(a.leg)                   as leg,
        MAX(a.home_win)              as home_win,
        MAX(a.away_win)              as away_win,
        MAX(a.home_draw)             as home_draw,
        MAX(a.away_draw)             as away_draw,
        MAX(a.home_lose)             as home_lose,
        MAX(a.away_lose)             as away_lose,
        MAX(a.home_gk)               as home_gk,
        MAX(a.away_gk)               as away_gk,
        MAX(a.home_goals)            as home_goals,
        MAX(a.away_goals)            as away_goals,
        MAX(a.home_assists)          as home_assists,
        MAX(a.away_assists)          as away_assists,
        MAX(a.home_pens_made)        as home_pens_made,
        MAX(a.away_pens_made)        as away_pens_made,
        MAX(a.home_pens_att)         as home_pens_att,
        MAX(a.away_pens_att)         as away_pens_att,
        MAX(a.home_xg)               as home_xg,
        MAX(a.away_xg)               as away_xg,
        MAX(a.home_clean_sheet)      as home_clean_sheet,
        MAX(a.away_clean_sheet)      as away_clean_sheet,
        MAX(a.home_shots)            as home_shots,
        MAX(a.away_shots)            as away_shots,
        MAX(a.home_shots_ot)         as home_shots_ot,
        MAX(a.away_shots_ot)         as away_shots_ot,
        MAX(a.home_cards_yellow)     as home_cards_yellow,
        MAX(a.away_cards_yellow)     as away_cards_yellow,
        MAX(a.home_cards_red)        as home_cards_red,
        MAX(a.away_cards_red)        as away_cards_red,
        MAX(a.home_cards_yellow_red) as home_cards_yellow_red,
        MAX(a.away_cards_yellow_red) as away_cards_yellow_red,
        MAX(a.home_captain)          as home_captain,
        MAX(a.away_captain)          as away_captain,
        MAX(a.home_minutes)          as home_minutes,
        MAX(a.away_minutes)          as away_minutes,
        MAX(a.home_started)          as home_started,
        MAX(a.away_started)          as away_started,
        MAX(a.home_sub_in)           as home_sub_in,
        MAX(a.away_sub_in)           as away_sub_in,

        0 as home_sub_out,
        oi.away_sub_out,

        0 as home_injured,
        oi.away_injured
    from away_stats a
    left join out_and_injured oi
    on a.id_match = oi.match and a.id_team = oi.team and a.id_player = oi.player_out
    group by a.id_comp, a.competition, a.id_match, a.id_player, a.id_team
)
insert into analytics.staging_players_performance
select *
from joined;