drop function if exists analytics.all_coaches_rankings;

create function analytics.all_coaches_rankings(in_comps character varying[], in_seasons character varying[], group_by_club boolean DEFAULT true, group_by_competition boolean DEFAULT true, group_by_season boolean DEFAULT true, first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Coach" character varying, "Club" character varying[], "Competition" character varying[], "Season" character varying[], "Matches" bigint, "Points" bigint, "Points/Match" numeric, "Wins" bigint, "Draws" bigint, "Loses" bigint, "% Wins" numeric, "% Loses" numeric, "Goals For" bigint, "Goals Against" bigint, "Goals Diff" bigint, "Clean Sheets" bigint, "xG For (fbref)" numeric, "xG For/Match (fbref)" numeric, "xG Against (fbref)" numeric, "xG Against/Match (fbref)" numeric, "Shots For" bigint, "Shots on Target For" bigint, "Shots Against" bigint, "Shots on Target Against" bigint, "Succ Passes" bigint, "Att Passes" bigint, "Shots/onTarget Conversion Rate For" numeric, "Shots/onTarget Conversion Rate Against" numeric, "Shots/Goals Conversion Rate For" numeric, "Shots/Goals Conversion Rate Against" numeric, "onTarget/Goals Conversion Rate For" numeric, "onTarget/Goals Conversion Rate Against" numeric, "Succ Passes Rate" numeric)
    language plpgsql
as
$$
DECLARE
	query text;
begin
	PERFORM analytics.check_side(side);

	RETURN QUERY
	/*with cup_ranking as (
		select *
		from analytics.cup_ranking(in_comp, in_seasons)
	),*/
	with coaches_stats as (
		SELECT
			CASE WHEN played_home THEN home_manager ELSE away_manager END AS "Coach",
			/*coalesce(stats.club, 'ALL')*/ array_agg(distinct stats.club) as "Club",
			/*coalesce(stats.competition, 'ALL')*/ array_agg(distinct stats.competition) as "Competition",
			/*coalesce(stats.season, 'ALL')*/ array_agg(distinct stats.season) AS "Season",

			case when grouping(stats.club) = 1 then 'ALL' else 'Single' end as grouping_clubs,
			case when grouping(stats.competition) = 1 then 'ALL' else 'Single' end as grouping_competitions,
			case when grouping(stats.season) = 1 then 'ALL' else 'Single' end as grouping_seasons,

			analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",
			analytics.set_bigint_stat(sum(home_points), sum(away_points), side) as "Points",

			analytics.set_bigint_stat(sum(home_win), sum(away_win), side) as "Wins",
			analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side) as "Draws",
			analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side) as "Loses",

			round(analytics.set_bigint_stat(sum(home_win), sum(away_win), side)::numeric / analytics.set_bigint_stat(sum(home_match), sum(away_match), side), 2) as "% Wins",
			round(analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side)::numeric / analytics.set_bigint_stat(sum(home_match), sum(away_match), side), 2) as "% Loses",

			analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side) as "Goals For",
			analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side) as "Goals Against",
			analytics.set_bigint_stat(sum(home_goals_for - home_goals_against), sum(away_goals_for - away_goals_against), side) as "Goals Diff",

			analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side) as "Clean Sheets",

			analytics.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side) as "xG For (fbref)",

			analytics.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side) as "xG Against (fbref)",

			--analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), side) as "Yellow Cards",
			--analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side) as "Red Cards",
			--analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side) as "Incl. 2 Yellow Cards",

			--analytics.set_bigint_stat(sum(home_fouls), sum(away_fouls), side) as "Fouls",

			analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), side) as "Shots For",
			analytics.set_bigint_stat(sum(home_shots_ot_for), sum(away_shots_ot_for), side) as "Shots on Target For",

			analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes",
			analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",

			analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), side) as "Shots Against",
			analytics.set_bigint_stat(sum(home_shots_ot_against), sum(away_shots_ot_against), side) as "Shots on Target Against"

		from analytics.staging_teams_performance as stats
		left join upper.championship chp
		on stats.id_comp = chp.id
		where CASE WHEN 'all' = ANY(ARRAY(SELECT lower(x) FROM unnest(in_comps) AS x)) and not 'bundesliga' = any(ARRAY(SELECT lower(x) FROM unnest(in_comps) AS x)) THEN true ELSE stats.competition = ANY(ARRAY(SELECT x FROM unnest(in_comps) AS x)) END
		and stats.season = any(in_seasons)
		and (
			(
				chp.id is not null
				and length(stats.week) <= 2
				and cast(stats.week as int) between first_week and last_week
			)
			or chp.id is null
		)
		and stats.date between first_date::date and last_date::date
		and case
			when side = 'neutral' then (round = 'Final')
			when side in ('home', 'away', 'both') then (round is null or round != 'Final')
			else true
		end
		AND (
			cardinality(day_slots) = 0
			OR TRIM(TO_CHAR(date, 'Day')) = ANY(day_slots)
		)
		AND (
			cardinality(time_slots) = 0
			OR LEFT(stats.time::text, 5) = ANY(time_slots)
		)

		group by case when played_home then home_manager else away_manager end, cube(stats.club, stats.competition, stats.season)
	)

	select
		cs."Coach",
		cs."Club",
		cs."Competition",
		cs."Season",

		cs."Matches",

		cs."Points",
		case
			when side = 'neutral'
			then null::numeric
			else case
				when cs."Matches" != 0
				then round(cs."Points" / cs."Matches"::numeric, r)
				else null::numeric
			end
		end as "Points/Match",

		cs."Wins",
		cs."Draws",
		cs."Loses",

		cs."% Wins",
		cs."% Loses",

		cs."Goals For",
		cs."Goals Against",
		cs."Goals Diff",

		cs."Clean Sheets",

		cs."xG For (fbref)",
		case
			when cs."Matches" != 0 then
				round(cs."xG For (fbref)" / cs."Matches"::numeric, r)
			else 0.0
		end as "xG For/Match (fbref)",

		cs."xG Against (fbref)",
		case
			when cs."Matches" != 0 then
				round(cs."xG Against (fbref)" / cs."Matches"::numeric, r)
			else 0.0
		end as "xG Against/Match (fbref)",

		--cs."Yellow Cards",
		--cs."Red Cards",
		--cs."Incl. 2 Yellow Cards",
		--cs."Fouls",

		cs."Shots For",
		cs."Shots on Target For",

		cs."Shots Against",
		cs."Shots on Target Against",

		cs."Succ Passes",
		cs."Att Passes",

		case when cs."Shots on Target For"     = 0 then 0.0 else round(cs."Shots For"::numeric / cs."Shots on Target For"::numeric, r)         end as "Shots/onTarget Conversion Rate For",
		case when cs."Shots on Target Against" = 0 then 0.0 else round(cs."Shots Against"::numeric / cs."Shots on Target Against"::numeric, r) end as "Shots/onTarget Conversion Rate Against",
		case when cs."Goals For"               = 0 then 0.0 else round(cs."Shots For"::numeric / cs."Goals For"::numeric, r)                   end as "Shots/Goals Conversion Rate For",
		case when cs."Goals Against"           = 0 then 0.0 else round(cs."Shots Against"::numeric / cs."Goals Against"::numeric, r)           end as "Shots/Goals Conversion Rate Against",
		case when cs."Goals For"               = 0 then 0.0 else round(cs."Shots on Target For"::numeric / cs."Goals For"::numeric, r)         end as "onTarget/Goals Conversion Rate For",
		case when cs."Goals Against"           = 0 then 0.0 else round(cs."Shots on Target Against"::numeric / cs."Goals Against"::numeric, r) end as "onTarget/Goals Conversion Rate Against",

		case when cs."Att Passes"              = 0 then 0.0 else round(cs."Succ Passes"::numeric / cs."Att Passes"::numeric, r)                end as "Succ Passes Rate"
	from coaches_stats cs
	where (
		(group_by_club AND cs.grouping_clubs != 'ALL')
   		OR (NOT group_by_club AND cs.grouping_clubs = 'ALL')
	)
	AND (
		(group_by_competition AND cs.grouping_competitions != 'ALL')
   		OR (NOT group_by_competition AND cs.grouping_competitions = 'ALL')
	)
	AND (
		(group_by_season AND cs.grouping_seasons != 'ALL')
   		OR (NOT group_by_season AND cs.grouping_seasons = 'ALL')
	);
	--left join cup_ranking cr
	--on ts.id_team = cr.id_team


end;
$$;
