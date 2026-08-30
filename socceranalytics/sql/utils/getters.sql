drop function if exists analytics.get_competition_ids;
drop function if exists analytics.get_competition_names;
drop function if exists analytics.get_last_opponent;
drop function if exists analytics.get_last_round_last_week;
drop function if exists analytics.get_player_age;
drop function if exists analytics.get_season_schemas;


create function analytics.get_competition_ids()
    returns TABLE("Competition" character varying)
    language plpgsql
as
$$
begin
    return query
	SELECT id as Competition
    from upper.competition
    where kind in ('championship', 'continental_cup')
    ;
end;
$$;

create function analytics.get_competition_names()
    returns TABLE("Competition" character varying)
    language plpgsql
as
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
$$;

create function analytics.get_last_opponent(id_club character varying, id_season character varying) returns character varying
    language plpgsql
as
$$
DECLARE
    season_schema text;
	opponent_name varchar(100);
	query text;
begin
	season_schema = 'season_' || id_season;

	query := format(
		'select c.complete_name
		into opponent_name
		from %I.match m
		join upper.club c
		on m.home_team = c.id  or m.away_team = c.id
		where (home_team = id_club or away_team = id_club)
		and m.season = id_season
		and c.complete_name  not in (
			select complete_name from upper.club where id = id_club
		)
		order by m.date desc
		limit 1;',
		season_schema
	);

	EXECUTE query INTO opponent_name USING id_club, id_season;

	return opponent_name;
end;
$$;

create function analytics.get_last_round_last_week(in_comp character varying, in_seasons character varying[])
    returns TABLE(id_team character varying, season character varying, competition character varying, last_round character varying, last_week character varying)
    language plpgsql
as
$$
begin
	RETURN QUERY
	select
		distinct on (stp.id_team, stp.season, stp.competition)
		stp.id_team,
		stp.season,
		stp.competition,
		CASE
	      WHEN stp.round = 'Final' AND (
	        (
	        	stp.played_home AND (
	        		stp.home_score > stp.away_score
	        		or stp.home_penalty_shootout_scored > stp.away_penalty_shootout_scored
	        	)
	        )
	        OR (
	        	NOT stp.played_home and (
		        	stp.away_score > stp.home_score
		        	or stp.away_penalty_shootout_scored > stp.home_penalty_shootout_scored
		        )
	        )
	      )
	      THEN 'Winner'
	      ELSE stp.round
	    END AS last_round,
		week as last_week
	from analytics.staging_teams_performance stp
	where case when lower(in_comp) like '%all%' then true else stp.competition = in_comp end
	and stp.season = any(in_seasons)
	order by stp.id_team, stp.season, stp.competition, stp.date desc;
end;
$$;

create function analytics.get_player_age(season character varying, birth_date date) returns numeric
    language plpgsql
as
$$
begin
	return COALESCE(
	    EXTRACT(
	        EPOCH FROM AGE(
	            CASE
	                WHEN season = (
	                  CASE
	                    WHEN current_date < TO_DATE(EXTRACT(YEAR FROM current_date)::text || '-07-01', 'YYYY-MM-DD')
	                    THEN (EXTRACT(YEAR FROM current_date) - 1)::text || '_' || EXTRACT(YEAR FROM current_date)::text
	                    ELSE EXTRACT(YEAR FROM current_date)::text || '_' || (EXTRACT(YEAR FROM current_date) + 1)::text
	                  END
	                )
	                THEN current_date
	                ELSE TO_DATE(split_part(season, '_', 2) || '-06-30', 'YYYY-MM-DD')
	            END,
	            birth_date
	        )
	    ) / (365.25 * 24 * 60 * 60),
	    0.0
	);
end;
$$;

create function analytics.get_season_schemas()
    returns TABLE("Schema" text)
    language plpgsql
as
$$
begin
    return query
	SELECT schema_name::text as "Schema"
    FROM information_schema.schemata
    WHERE schema_name ~ 'season_\d';
end;
$$;
