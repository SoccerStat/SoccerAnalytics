drop function if exists teams_justice_table;

create or replace function teams_justice_table(
	in id_chp varchar(100),
	in id_season varchar(20),
	in first_week int default 1,
	in last_week int default 100/*,
	in side ranking_type default 'both'*/
)
returns table(
    "id_match" varchar(20),
    "Club" varchar(100),
    "played_home" boolean,
    "xg" numeric,
    "shots" bigint
)
as $$
begin
    if id_chp not in (select id from championship) then
		raise exception 'Invalid value for id_chp. Valid values are ligue_1, premier_league, serie_a, la_liga, fussball_bundesliga';
	end if;
	if id_season  !~ '^\d{4}-(\d{4})$' or (substring(id_season, 1, 4)::int + 1)::text != substring(id_season, 6, 4) then 
		raise exception 'Wrong format of season. It should be like "2022-2023".';
	end if;
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week';
	end if;
	/*if side not in ('home', 'away', 'both') then
        raise exception 'Invalid value for ranking_type. Valid values are: home, away, both';
    end if;*/
   
	return query
    select 
        ts.id_match, 
        c.complete_name as "Club", 
        ts.played_home, 
        ts.xg::numeric, 
        ts.shots::bigint
    from team_stats ts 
    join club c on ts.id_team = c.id 
    join match m on m.id = ts.id_match 
    where
        m.id_championship = id_chp and 
        m.season = id_season and

        length(m.week) <= 2 and
        cast(m.week as int) >= first_week and
        cast(m.week as int) <= last_week /*and
        
        case
            when side = 'home' then ts.played_home = true
            when side = 'away' then ts.played_home = false
            else true
        end;*/;
end;
$$ language plpgsql;