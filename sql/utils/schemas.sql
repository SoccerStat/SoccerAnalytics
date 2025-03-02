create or replace function dwh_utils.get_season_schemas(
)
returns table("Schema" text) as
$$
begin
    return query 
	SELECT schema_name::text as "Schema"
    FROM information_schema.schemata 
    WHERE schema_name ~ 'dwh_\d';


end;
$$
language plpgsql;