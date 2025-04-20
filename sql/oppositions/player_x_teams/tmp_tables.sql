drop table if exists tmp_players_opposition;
create temp table tmp_players_opposition (
    season varchar(20),
    id_comp varchar(100),
    competition varchar(100),

    player varchar(20),
    team varchar(100),

    opponent varchar(100),
    
    home_match int,
    away_match int,

    home_win int,
    away_win int,

    home_draw int,
    away_draw int,

    home_lose int,
    away_lose int,

    home_goals int,
    away_goals int,

    home_assists int,
    away_assists int,

    home_minutes int,
    away_minutes int,

    home_shots int,
    away_shots int,

    home_on_target int,
    away_on_target int,

    home_y_cards int,
    away_y_cards int,
    home_yr_cards int,
    away_yr_cards int,
    home_r_cards int,
    away_r_cards int
);