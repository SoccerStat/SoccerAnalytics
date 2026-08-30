drop function if exists analytics.set_bigint_stat;
drop function if exists analytics.set_numeric_stat;

create function analytics.set_bigint_stat(home_stat bigint, away_stat bigint, side analytics.side) returns bigint
    language plpgsql
as
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
$$;

create function set_numeric_stat(home_stat numeric, away_stat numeric, side analytics.side) returns numeric
    language plpgsql
as
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
$$;