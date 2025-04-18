create or replace function analytics.get_season_schemas(
)
returns table("Schema" text) as
$$
begin
    return query 
	SELECT schema_name::text as "Schema"
    FROM information_schema.schemata 
    WHERE schema_name ~ 'season_\d';


end;
$$
language plpgsql;