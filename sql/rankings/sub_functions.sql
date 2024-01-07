drop function if exists set_bigint_stat;
drop function if exists set_numeric_stat;
drop function if exists get_last_opponent;
drop type if exists ranking_type CASCADE;
drop type if exists team_ranking CASCADE;
drop type if exists player_ranking CASCADE;

create type ranking_type as enum ('home', 'away', 'both');
create type team_ranking as enum ('Points', 'Wins', 'Draws', 'Loses', 'Goals For', 'Goals Against', 'Goals Diff', 'xG', 'Yellow Cards', 'Red Cards', 'Fouls');
create type player_ranking as enum ('scorer', 'assist');

create or replace function set_bigint_stat(
	in home_stat bigint,
	in away_stat bigint,
	in side ranking_type
)
returns bigint 
as $$
begin
	if side = 'home' then 
		return home_stat;
	elsif side = 'away' then
		return away_stat;
	else 
		return home_stat + away_stat;
	end if;
end;
$$ language plpgsql;

create or replace function set_numeric_stat(
	in home_stat numeric,
	in away_stat numeric,
	in side ranking_type
)
returns numeric 
as $$
begin
	if side = 'home' then 
		return home_stat;
	elsif side = 'away' then
		return away_stat;
	else 
		return home_stat + away_stat;
	end if;
end;
$$ language plpgsql;

/*
 * TODO: A revoir // Ajouter paramètres first_week, last_week pour choper le dernier adversaire de la période sélectionnée
 */
create or replace function get_last_opponent(
	in id_club varchar(20),
	in id_season varchar(20)
)
returns varchar(100)
as $$
declare opponent_name varchar(100);
begin 
	select c.complete_name
	into opponent_name
	from match m
	join club c 
	on m.home_team = c.id  or m.away_team = c.id
	where (home_team = id_club or away_team = id_club)
	and m.season = id_season
	and c.complete_name  not in (
		select complete_name from club where id = id_club
	)
	order by m.date desc
	limit 1;

	return opponent_name;
end;
$$ language plpgsql;