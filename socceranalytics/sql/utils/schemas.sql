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
    --and substr(schema_name::text, 8, 9) >= '2018_2019'
    ;


end;
$$
language plpgsql;