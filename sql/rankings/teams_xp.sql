create or replace function dwh_utils.teams_justice_table(
	in id_comp varchar(100),
	in id_season varchar(20),
	in first_week int,
	in last_week int
)
returns table(
    "match" varchar(20),
    "Club" varchar(100),
    "played_home" boolean,
    "xg" numeric,
    "shots" bigint
)
as $$
DECLARE
    season_schema text;
    query text;
begin
    if id_comp not in (select id from dwh_upper.championship) then
		raise exception 'Invalid value for id_comp. Valid values are ligue_1, premier_league, serie_a, la_liga, fussball_bundesliga';
	end if;
	if id_season  !~ '^\d{4}_(\d{4})$' or (substring(id_season, 1, 4)::int + 1)::text != substring(id_season, 6, 4) then 
		raise exception 'Wrong format of season. It should be like "2022_2023".';
	end if;
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week';
	end if;

    season_schema = 'dwh_' || id_season;
   
	query := format(
        'select 
            ts.match, 
            c.name as "Club", 
            ts.played_home, 
            ts.xg::numeric, 
            ts.nb_shots_total::bigint
        from %I.team_stats ts 
        join %I.match m on m.id = ts.match 
        join dwh_upper.club c on ts.team = m.competition || ''_'' || c.id
        where
            m.competition = ''' || id_comp || ''' and 

            length(m.week) <= 2 and
            cast(m.week as int) >= ''' || first_week || ''' and
            cast(m.week as int) <= ''' || last_week || ''';',
        season_schema, season_schema
    );

    RETURN QUERY EXECUTE query USING id_comp, id_season, first_week, last_week;

end;
$$ language plpgsql;