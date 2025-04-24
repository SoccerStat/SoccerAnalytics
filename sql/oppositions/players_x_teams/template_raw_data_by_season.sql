with selected_matches as (
    select m.id, m.home_team, m.away_team, m.competition as comp, coalesce(chp.name, c_cup.name) as competition
    from season_{season}.match m
    left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
    where m.competition = '{comp}'
),
home_oppositions as (
    select
        '{season}' as season,
        m.comp,
        m.competition,

        ps.player,
        m.home_team as team,

        m.away_team as opponent,

        1 as home_match,
        0 as away_match,

        case
            when tsh.score > tsa.score then 1
            else 0
        end as home_win,
        0 as away_win,
        case
            when tsh.score = tsa.score then 1
            else 0
        end as home_draw,
        0 as away_draw,
        case
            when tsh.score < tsa.score then 1
            else 0
        end as home_lose,
        0 as away_lose,

        ps.nb_goals as home_goals,
        0 as away_goals,

        ps.nb_assists as home_assists,
        0 as away_assists,

        ps.nb_minutes as home_minutes,
        0 as away_minutes,

        ps.nb_shots as home_shots,
        0 as away_shots,

        ps.nb_shots_on_target as home_on_target,
        0 as away_on_target,

        ps.nb_cards_yellow as home_y_cards,
        0 as away_y_cards,
        ps.nb_cards_second_yellow as home_yr_cards,
        0 as away_yr_cards,
        ps.nb_cards_red as home_r_cards,
        0 as away_r_cards
    
    from (select * from season_{season}.player_main_stats where played_home) as ps
    join selected_matches m
    on ps.match = m.id
    join (select * from season_{season}.team_stats where played_home) as tsh
    on tsh.match = m.id
    join (select * from season_{season}.team_stats where not played_home) as tsa
    on tsa.match = m.id
)
insert into tmp_players_opposition
select *
from home_oppositions;

with selected_matches as (
    select m.id, m.home_team, m.away_team, m.competition as comp, coalesce(chp.name, c_cup.name) as competition
    from season_{season}.match m
    left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
    where m.competition = '{comp}'
),
away_oppositions as (
    select
        '{season}' as season,
        m.comp,
        m.competition,

        ps.player,
        m.away_team as team,

        m.home_team as opponent,

        0 as home_match,
        1 as away_match,

        0 as home_win,
        case
            when tsa.score > tsh.score then 1
            else 0
        end as away_win,

        0 as home_draw,
        case
            when tsa.score = tsh.score then 1
            else 0
        end as away_draw,

        0 as home_lose,
        case
            when tsa.score < tsh.score then 1
            else 0
        end as away_lose,

        0 as home_goals,
        ps.nb_goals as away_goals,

        0 as home_assists,
        ps.nb_assists as away_assists,

        0 as home_minutes,
        ps.nb_minutes as away_minutes,

        0 as home_shots,
        ps.nb_shots as away_shots,

        0 as home_on_target,
        ps.nb_shots_on_target as away_on_target,

        0 as home_y_cards,
        ps.nb_cards_yellow as away_y_cards,
        0 as home_yr_cards,
        ps.nb_cards_second_yellow as away_yr_cards,
        0 as home_r_cards,
        ps.nb_cards_red as away_r_cards
    
    from (select * from season_{season}.player_main_stats where not played_home) as ps
    join selected_matches m
    on ps.match = m.id
    join (select * from season_{season}.team_stats where played_home) as tsh
    on tsh.match = m.id
    join (select * from season_{season}.team_stats where not played_home) as tsa
    on tsa.match = m.id
)
insert into tmp_players_opposition
select *
from away_oppositions;