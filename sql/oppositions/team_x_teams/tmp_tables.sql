drop table if exists tmp_teams_opposition;
create temp table tmp_teams_opposition (
    season varchar(20),
    id_comp varchar(100),
    competition varchar(100),

    team varchar(100),
    opponent varchar(100),

    home_match int,
    away_match int,

    home_goals_for int,
    away_goals_for int,
    home_goals_against int,
    away_goals_against int,

    home_win int,
    away_win int,

    home_draw int,
    away_draw int,

    home_lose int,
    away_lose int,

    home_shots_for int,
    away_shots_for int,
    home_on_target_for int,
    away_on_target_for int,

    away_shots_against int,
    home_shots_against int,
    away_on_target_against int,
    home_on_target_against int,

    home_y_cards int,
    away_y_cards int,
    home_yr_cards int,
    away_yr_cards int,
    home_r_cards int,
    away_r_cards int
);