drop type if exists dwh_utils.team_ranking CASCADE;
drop type if exists dwh_utils.player_ranking CASCADE;
drop type if exists dwh_utils.ranking_type CASCADE;


create type dwh_utils.team_ranking as enum ('Points', 'Wins', 'Draws', 'Loses', 'Goals For', 'Goals Against', 'Goals Diff', 'xG', 'Yellow Cards', 'Red Cards', 'Fouls');
create type dwh_utils.player_ranking as enum ('scorer', 'assist');
create type dwh_utils.ranking_type as enum ('home', 'away', 'both');
