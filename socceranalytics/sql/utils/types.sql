drop type if exists analytics.team_ranking CASCADE;
drop type if exists analytics.player_ranking CASCADE;
drop type if exists analytics.side CASCADE;


create type analytics.team_ranking as enum ('Points', 'Wins', 'Draws', 'Loses', 'Goals For', 'Goals Against', 'Goals Diff', 'xG', 'Yellow Cards', 'Red Cards', 'Fouls');
create type analytics.player_ranking as enum ('scorer', 'assist');
create type analytics.side as enum ('home', 'away', 'both');
