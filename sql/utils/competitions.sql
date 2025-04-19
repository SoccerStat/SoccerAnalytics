create or replace function analytics.get_competitions(
)
returns table("Competition" varchar(100)) as
$$
begin
    return query 
	SELECT id as Competition
    from upper.competition
    ;
end;
$$
language plpgsql;