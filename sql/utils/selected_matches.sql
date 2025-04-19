create or replace function analytics.selected_matches(
        in season varchar(20),
        in id_comp varchar(100),
        in first_week int default 1,
        in last_week int default 100,
        in first_date date default '1970-01-01',
        in last_date date default '2099-12-31'
)
returns table(
    id varchar(20),
    home_team varchar(20),
    away_team varchar(20),
    attendance int,
    competition varchar(100)
) as
$$
DECLARE
	query text;
begin
    query := format(
        '
        select m.id, m.home_team, m.away_team, m.attendance, m.competition
        from season_' || season || '.match m
        left join upper.championship c
        on m.competition = c.id
        where
            competition = ''' || id_comp || '''
            and (
                (
                    c.id is not null
                    and cast(week as int) between ''' || first_week || ''' and ''' || last_week || '''
                    and length(week) <= 2
                )
                or c.id is null
            )
            and m.date between ''' || first_date || ''' and ''' || last_date || ''';
        '
    );
    RETURN QUERY EXECUTE query USING season, id_comp, first_week, last_week, first_date, last_date;
end;
$$
language plpgsql;