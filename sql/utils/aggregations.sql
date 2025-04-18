drop function if exists analytics.set_bigint_stat;
drop function if exists analytics.set_numeric_stat;
drop function if exists analytics.get_last_opponent;


create or replace function analytics.set_bigint_stat(
	in home_stat bigint,
	in away_stat bigint,
	in side analytics.ranking_type
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


create or replace function analytics.set_numeric_stat(
	in home_stat numeric,
	in away_stat numeric,
	in side analytics.ranking_type
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
create or replace function analytics.get_last_opponent(
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
	season_schema = 'season_' || id_season;

	query := format(
		'select c.complete_name
		into opponent_name
		from %I.match m
		join upper.club c 
		on m.home_team = c.id  or m.away_team = c.id
		where (home_team = id_club or away_team = id_club)
		and m.season = id_season
		and c.complete_name  not in (
			select complete_name from upper.club where id = id_club
		)
		order by m.date desc
		limit 1;',
		season_schema
	);

	EXECUTE query INTO opponent_name USING id_club, id_season;

	return opponent_name;
end;
$$ language plpgsql;