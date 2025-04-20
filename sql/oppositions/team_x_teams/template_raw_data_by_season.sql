with selected_match as (
    select m.id, m.home_team, m.away_team, m.competition as id_comp, coalesce(chp.name, c_cup.name) as competition
    from season_{season}.match m
    left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
    where m.competition = '{id_comp}'
),
home_oppositions as (
    select
        '{season}' as season,
        m.id_comp,
        m.competition,

        m.home_team as id_team,
        m.away_team as id_opponent,

        1 as home_match,
        0 as away_match,

        tsh.score as home_goals_for,
        0 as away_goals_for,
        tsa.score as home_goals_against,
        0 as away_goals_against,

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

        tsh.nb_shots_total as home_shots_for,
        0 as away_shots_for,
        tsh.nb_shots_on_target as home_on_target_for,
        0 as away_on_target_for,

        0 as away_shots_against,
        tsa.nb_shots_total as home_shots_against,
        0 as away_on_target_against,
        tsa.nb_shots_on_target as home_on_target_against,

        tsh.nb_cards_yellow as home_y_cards,
        0 as away_y_cards,
        tsh.nb_cards_second_yellow as home_yr_cards,
        0 as away_yr_cards,
        tsh.nb_cards_red as home_r_cards,
        0 as away_r_cards

    from selected_match m
    join (select * from season_{season}.team_stats where played_home) tsh
    on m.id = tsh.match
    join (select * from season_{season}.team_stats where not played_home) tsa
    on m.id = tsa.match
)
insert into tmp_teams_opposition
select *
from home_oppositions;

with selected_match as (
    select m.id, m.home_team, m.away_team, m.competition as id_comp, coalesce(chp.name, c_cup.name) as competition
    from season_{season}.match m
    left join upper.championship chp
	on m.competition = chp.id
	left join upper.continental_cup c_cup
	on m.competition = c_cup.id
    where m.competition = '{id_comp}'
),
away_oppositions as (
    select
        '{season}' as season,
        m.id_comp,
        m.competition,

        m.away_team as id_team,
        m.home_team as id_opponent,

        0 as home_match,
        1 as away_match,

        0 as home_goals_for,
        tsa.score as away_goals_for,
        0 as home_goals_against,
        tsh.score as away_goals_against,

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

        0 as home_shots_for,
        tsa.nb_shots_total as away_shots_for,
        0 as home_on_target_for,
        tsa.nb_shots_on_target as away_on_target_for,

        tsh.nb_shots_total as home_shots_against,
        0 as away_shots_against,
        tsh.nb_shots_on_target as home_on_target_against,
        0 as away_on_target_against,

        0 as home_y_cards,
        tsa.nb_cards_yellow as away_y_cards,
        0 as home_yr_cards,
        tsa.nb_cards_second_yellow as away_yr_cards,
        0 as home_r_cards,
        tsa.nb_cards_red as away_r_cards

    from selected_match m
    join (select * from season_{season}.team_stats where played_home) tsh
    on m.id = tsh.match
    join (select * from season_{season}.team_stats where not played_home) tsa
    on m.id = tsa.match
)
insert into tmp_teams_opposition
select *
from away_oppositions;