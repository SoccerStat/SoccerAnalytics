drop function if exists analytics.get_season_schemas;

create or replace function analytics.get_season_schemas(
)
returns table("Schema" text) as
$$
begin
    return query 
	SELECT schema_name::text as "Schema"
    FROM information_schema.schemata 
    WHERE schema_name ~ 'season_\d'
    -- AND case
	-- 	when extract(month from current_date) > 7
	-- 	then substr(schema_name::text, 8, 9) >= extract(year from current_date) || '_' || extract(year from current_date + interval '1 year')
	-- 	else substr(schema_name::text, 8, 9) >= extract(year from current_date - interval '1 year') || '_' || extract(year from current_date)
    -- end
    ;


end;
$$
language plpgsql;