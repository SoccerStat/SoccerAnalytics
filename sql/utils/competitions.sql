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