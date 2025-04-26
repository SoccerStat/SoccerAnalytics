create or replace function analytics.get_competition_ids(
)
returns table("Competition" varchar(100)) as
$$
begin
    return query 
	SELECT id as Competition
    from upper.competition
    where kind in ('championship', 'continental_cup')
    ;
end;
$$
language plpgsql;

create or replace function analytics.get_competition_names(
)
returns table("Competition" varchar(100)) as
$$
begin
    return query 
	select name
    from upper.championship
    union all
    select name
    from upper.continental_cup
    ;
end;
$$
language plpgsql;