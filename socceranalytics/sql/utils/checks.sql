drop function if exists analytics.check_coaches_ranking;
drop function if exists analytics.check_comp;
drop function if exists analytics.check_dates;
drop function if exists analytics.check_id_comp;
drop function if exists analytics.check_offensive_players_ranking;
drop function if exists analytics.check_season;
drop function if exists analytics.check_teams_ranking;
drop function if exists analytics.check_weeks;
drop function if exists analytics.check_side;

create function analytics.check_coaches_ranking(ranking character varying) returns void
    language plpgsql
as
$$
begin
	if ranking not in (
		'Minutes',
		'Attendance',
		'Matches',
		'Points',
		'Points/Match',
		'Wins',
		'Draws',
		'Loses',
		'% Wins',
		'% Loses',
		'Goals For',
		'Goals Against',
		'Goals Diff',
		'Clean Sheets',
		'xG For (fbref)',
		'xG For /Match (fbref)',
		'xG Against (fbref)',
		'xG Against /Match (fbref)',
		'xG For (understat)',
		'xG For /Match (understat)',
		'xG Against (understat)',
		'xG Against /Match (understat)',
		'xG For',
		'xG For /Match',
		'xG Against',
		'xG Against /Match',
		'Yellow Cards',
		'Red Cards',
		'Incl. 2 Yellow Cards',
		'Fouls',

		'Shots For',
		'Shots on Target For',
		'Shots Against',
		'Shots on Target Against',

		'Succ Passes',
		'Att Passes',

	    'Shots/onTarget Conversion Rate For',
	 	'Shots/onTarget Conversion Rate Against',
	 	'Shots/Goals Conversion Rate For',
	 	'Shots/Goals Conversion Rate Against',
	 	'onTarget/Goals Conversion Rate For',
	 	'onTarget/Goals Conversion Rate Against',

		'Succ Passes Rate'
	)
	then
		raise exception 'Invalid value for ranking: %.', ranking;
	end if;
end;
$$;

create or replace function analytics.check_comp(
	in comp varchar(100)
)
returns void as
$$
begin
	if comp not in (select name from upper.championship)
	and comp not in (select name from upper.continental_cup)
	then
		raise exception 'Invalid value for comp.';
	end if;
end;
$$
language plpgsql;

create or replace function analytics.check_dates(
	in first_date varchar(20),
	in last_date varchar(20)
)
returns void as
$$
declare
	d1 date;
	d2 date;
begin
	begin
        d1 := TO_DATE(first_date, 'YYYY-MM-DD');
    exception when others then
        raise exception 'Invalid first_date format. Use YYYY-MM-DD.';
    end;

	begin
        d2 := TO_DATE(last_date, 'YYYY-MM-DD');
    exception when others then
        raise exception 'Invalid last_date format. Use YYYY-MM-DD.';
    end;

	if d1 > d2 then
		raise exception 'Choose first_date as being lower than last_date.';
	end if;
end;
$$
language plpgsql;

create function analytics.check_id_comp(id_comp character varying) returns void
    language plpgsql
as
$$
begin
	if id_comp not in (select id from upper.championship)
	and id_comp not in (select id from upper.continental_cup)
	then
		raise exception 'Invalid value for id_comp.';
	end if;
end;
$$;

create function analytics.check_offensive_players_ranking(ranking character varying) returns void
    language plpgsql
as
$$
begin
	if ranking not in (
		'Minutes',
		'Matches',
		'Started',
		'Sub In',
		'Sub Out',
		'Injured',
		'Captain',
		'Wins',
		'Draws',
		'Loses',
		'Goals',
		'Att Penalties',
		'Succ Penalties',
		'Assists',
		'xG (fbref)',
		'xG (fbref) / Match',
		'xG (understat)',
		'xG (understat) / Match',

		'Yellow Cards',
		'Red Cards',
		'Incl. 2 Yellow Cards',

		'Shots',
		'Shots on Target',

		'Succ Passes',
		'Att Passes',

	    'Shots/onTarget Conversion Rate',
	 	'Shots/Goals Conversion Rate',
	 	'onTarget/Goals Conversion Rate',

		'Succ Passes Rate'
	)
	then
		raise exception 'Invalid value for ranking: %.', ranking;
	end if;
end;
$$;

create or replace function analytics.check_season(
	in season varchar(20)
)
returns void as
$$
begin
	if season  !~ '^\d{4}_(\d{4})$' or (substring(season, 1, 4)::int + 1)::text != substring(season, 6, 4) then 
		raise exception 'Wrong format of season. It should be like "2022_2023".';
	end if;
end;
$$
language plpgsql;

create or replace function analytics.check_side(
	in side analytics.side
)
returns void as
$$
begin
	if side not in ('home', 'away', 'both', 'neutral', 'all') then
        raise exception 'Invalid value for side. Valid values are: home, away, both, neutral, all.';
    end if;
end;
$$
language plpgsql;

create function analytics.check_teams_ranking(ranking character varying) returns void
    language plpgsql
as
$$
begin
	if ranking not in (
		'Minutes',
		'Attendance',
		'Matches',
		'Points',
		'Points/Match',
		'Wins',
		'Draws',
		'Loses',
		'Goals For',
		'Goals Against',
		'Goals Diff',
		'Clean Sheets',
		'xG For (fbref)',
		'xG For /Match (fbref)',
		'xG Against (fbref)',
		'xG Against /Match (fbref)',
		'xG For (understat)',
		'xG For /Match (understat)',
		'xG Against (understat)',
		'xG Against /Match (understat)',
		'xG For',
		'xG For /Match',
		'xG Against',
		'xG Against /Match',
		'Yellow Cards',
		'Red Cards',
		'Incl. 2 Yellow Cards',
		'Fouls',

		'Shots For',
		'Shots on Target For',
		'Shots Against',
		'Shots on Target Against',

		'Succ Passes',
		'Att Passes',

	    'Shots/onTarget Conversion Rate For',
	 	'Shots/onTarget Conversion Rate Against',
	 	'Shots/Goals Conversion Rate For',
	 	'Shots/Goals Conversion Rate Against',
	 	'onTarget/Goals Conversion Rate For',
	 	'onTarget/Goals Conversion Rate Against',

		'Succ Passes Rate'
	)
	then
		raise exception 'Invalid value for ranking: %.', ranking;
	end if;
end;
$$;

create or replace function analytics.check_weeks(
	in first_week int,
	in last_week int
)
returns void as
$$
begin
	if first_week > last_week then
		raise exception 'Choose first_week as being lower than last_week.';
	end if;
end;
$$
language plpgsql;
