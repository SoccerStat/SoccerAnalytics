drop function if exists check_parameters;
drop function if exists set_bigint_stat;
drop function if exists set_numeric_stat;
drop function if exists get_last_opponent;
drop type if exists ranking_type CASCADE;
drop type if exists team_ranking CASCADE;
drop type if exists player_ranking CASCADE;

create type public.ranking_type as enum ('home', 'away', 'both');
create type public.team_ranking as enum ('Points', 'Wins', 'Draws', 'Loses', 'Goals For', 'Goals Against', 'Goals Diff', 'xG', 'Yellow Cards', 'Red Cards', 'Fouls');
create type public.player_ranking as enum ('scorer', 'assist');

create or replace function public.check_parameters(
	in id_comp varchar(100),
	in id_season varchar(20),
	in first_week int,
	in last_week int,
	in side ranking_type
)
returns void as
$$
begin
    /*if which not in ('scorer', 'assist') then
		raise exception 'Invalid value for the type of player ranking. Valid values for "which" parameter are scorer, assist.';
	end if;*/
	if id_comp not in (select id from dwh_upper.championship) then
		raise exception 'Invalid value for id_comp. Valid values are ligue_1, premier_league, serie_a, la_liga, fussball_bundesliga';
	end if;
	if id_season  !~ '^\d{4}_(\d{4})$' or (substring(id_season, 1, 4)::int + 1)::text != substring(id_season, 6, 4) then 
		raise exception 'Wrong format of season. It should be like "2022_2023".';
	end if;
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week';
	end if;
	if side not in ('home', 'away', 'both') then
        raise exception 'Invalid value for ranking_type. Valid values are: home, away, both';
    end if;
end;
$$
language plpgsql;


create or replace function public.set_bigint_stat(
	in home_stat bigint,
	in away_stat bigint,
	in side ranking_type
)
returns bigint as 
$$
begin
	if side = 'home' then 
		return home_stat;
	elsif side = 'away' then
		return away_stat;
	else 
		return home_stat + away_stat;
	end if;
end;
$$ 
language plpgsql;


create or replace function public.set_numeric_stat(
	in home_stat numeric,
	in away_stat numeric,
	in side ranking_type
)
returns numeric as
$$
begin
	if side = 'home' then 
		return home_stat;
	elsif side = 'away' then
		return away_stat;
	else 
		return home_stat + away_stat;
	end if;
end;
$$ 
language plpgsql;


/*
 * TODO: A revoir // Ajouter paramètres first_week, last_week pour choper le dernier adversaire de la période sélectionnée
 */
create or replace function public.get_last_opponent(
	in id_club varchar(20),
	in id_season varchar(20)
)
returns varchar(100)
as $$
declare 
    season_schema text;
	opponent_name varchar(100);
	query text;
begin
	season_schema = 'dwh_' || id_season;

	query := format(
		'select c.complete_name
		into opponent_name
		from %I.match m
		join dwh_upper.club c 
		on m.home_team = c.id  or m.away_team = c.id
		where (home_team = id_club or away_team = id_club)
		and m.season = id_season
		and c.complete_name  not in (
			select complete_name from dwh_upper.club where id = id_club
		)
		order by m.date desc
		limit 1;',
		season_schema
	);

	EXECUTE query INTO opponent_name USING id_club, id_season;

	return opponent_name;
end;
$$ language plpgsql;