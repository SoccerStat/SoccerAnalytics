drop function if exists analytics.check_id_comp;
drop function if exists analytics.check_id_season;
drop function if exists analytics.check_weeks;
drop function if exists analytics.check_side;


create or replace function analytics.check_id_comp(
	in id_comp varchar(100)
)
returns void as
$$
begin
	if id_comp not in (select id from upper.championship)
	and id_comp not in (select id from upper.continental_cup)
	then
		raise exception 'Invalid value for id_comp.';
	end if;
end;
$$
language plpgsql;

-- create or replace function analytics.check_id_season(
-- 	in id_season varchar(20)
-- )
-- returns void as
-- $$
-- begin
-- 	if id_season  !~ '^\d{4}_(\d{4})$' or (substring(id_season, 1, 4)::int + 1)::text != substring(id_season, 6, 4) then 
-- 		raise exception 'Wrong format of season. It should be like "2022_2023".';
-- 	end if;
-- end;
-- $$
-- language plpgsql;

create or replace function analytics.check_weeks(
	in first_week int,
	in last_week int
)
returns void as
$$
begin
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week.';
	end if;
end;
$$
language plpgsql;

create or replace function analytics.check_side(
	in side analytics.ranking_type
)
returns void as
$$
begin
	if side not in ('home', 'away', 'both') then
        raise exception 'Invalid value for ranking_type. Valid values are: home, away, both.';
    end if;
end;
$$
language plpgsql;