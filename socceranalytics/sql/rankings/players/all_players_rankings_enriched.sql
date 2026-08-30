drop function if exists analytics.all_players_rankings_enriched;

create function analytics.all_players_rankings_enriched(in_comps character varying[], in_seasons character varying[], group_by_club boolean DEFAULT true, group_by_competition boolean DEFAULT true, group_by_season boolean DEFAULT true, first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Player" character varying, "Club" character varying[], "Competition" character varying[], "Season" character varying[], "Age" integer, "Height" integer, "Weight" integer, "Footed" character varying, "Nationalities" character varying[], "Matches" bigint, "Minutes" bigint, "Minutes/Match" numeric, "Captain" bigint, "Started" bigint, "Sub In" bigint, "Sub Out" bigint, "Wins" bigint, "Draws" bigint, "Loses" bigint, "Goals" bigint, "Assists" bigint, "Succ Penalties" bigint, "Att Penalties" bigint, "Rate Penalties" numeric, "xG (fbref)" numeric, "xG (fbref)/90" numeric, "Clean Sheets" bigint, "Yellow Cards" bigint, "Red Cards" bigint, "Incl. 2nd Yellow Cards" bigint)
    language plpgsql
as
$$
DECLARE
	query text;
begin
	PERFORM analytics.check_side(side);

-- Granularity Club
-- Si liste complète: 'TCC' (Tous Clubs Confondus)
-- Sinon : 'Club'

-- Granularity_competition
-- Si liste complète : 'TCC' (Toutes Compétitions Confondues)
-- Sinon : 'Competition'

	RETURN QUERY
	with players_stats as (
		select
			stats.player as "Player",
			array_agg(distinct stats.club) as "Club",
			array_agg(distinct stats.competition) as "Competition",
			array_agg(distinct stats.season) as "Season",

			case when grouping(stats.club) = 1 then 'ALL' else 'Single' end as grouping_clubs,
			case when grouping(stats.competition) = 1 then 'ALL' else 'Single' end as grouping_competitions,
			case when grouping(stats.season) = 1 then 'ALL' else 'Single' end as grouping_seasons,

			MAX(stats.age) AS "Age",
			MAX(stats.height) as "Height",
			MAX(stats.weight) as "Weight",
			MAX(stats.footed)::varchar as "Footed",
			MAX(stats.nationalities) as "Nationalities",

			analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), side) as "Minutes",
			analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",
			analytics.set_bigint_stat(sum(home_started), sum(away_started), side) as "Started",
			analytics.set_bigint_stat(sum(home_sub_in), sum(away_sub_in), side) as "Sub In",
			analytics.set_bigint_stat(sum(home_sub_out), sum(away_sub_out), side) as "Sub Out",
			analytics.set_bigint_stat(sum(home_injured), sum(away_injured), side) as "Injured",
			analytics.set_bigint_stat(sum(home_captain), sum(away_captain), side) as "Captain",

			analytics.set_bigint_stat(sum(home_win), sum(away_win), side) as "Wins",
			analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side) as "Draws",
			analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side) as "Loses",

			analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side) as "Clean Sheets",

			analytics.set_bigint_stat(sum(home_goals), sum(away_goals), side) as "Goals",
			analytics.set_bigint_stat(sum(home_pens_att), sum(away_pens_att), side) as "Att Penalties",
			analytics.set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) as "Succ Penalties",
			analytics.set_bigint_stat(sum(home_assists), sum(away_assists), side) as "Assists",

			analytics.set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as "xG (fbref)",

			analytics.set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), side) as "Yellow Cards",
			analytics.set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side) as "Red Cards",
			analytics.set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side) as "Incl. 2 Yellow Cards",

			analytics.set_bigint_stat(sum(home_shots), sum(away_shots), side) as "Shots",
			analytics.set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), side) as "Shots on Target",

			analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",
			analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes"

		from analytics.staging_players_performance as "stats"
		left join upper.championship chp -- TODO: remove it when cups are regarded
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
		group by
			(split_part(stats.id_player, '_', 1), stats.player),
			cube(stats.club, stats.competition, stats.season)
		--having (
		--	grouping(c.name) = 0 OR count(distinct c.name) > 1
		--) and (
		--	grouping(stats.competition) = 0 OR count(distinct stats.competition) > 1
		--)
	)
	select
		ps."Player",
		ps."Club",
		ps."Competition",
		ps."Season",

		ps."Age",
		ps."Height",
		ps."Weight",
		ps."Footed",
		ps."Nationalities",

		ps."Matches",
		ps."Minutes",
		case
			when ps."Matches" <> 0 then round(ps."Minutes"::numeric / ps."Matches", r)
			else 0.0
		end as "Minutes/Match",
		ps."Captain",
		ps."Started",
		ps."Sub In",
		ps."Sub Out",

		ps."Wins",
		ps."Draws",
		ps."Loses",

		ps."Goals",
		ps."Assists",
		ps."Succ Penalties",
		ps."Att Penalties",
		case
			when ps."Att Penalties" <> 0 then round(ps."Succ Penalties"::numeric / ps."Att Penalties", r)
			else 0.0
		end as "Rate Penalties",

		ps."xG (fbref)",
		case
			when ps."Matches" <> 0 then round(ps."xG (fbref)" / ps."Matches", r)
			else 0.0
		end as "xG (fbref)/90",

		ps."Clean Sheets",

		ps."Yellow Cards",
		ps."Red Cards",
		ps."Incl. 2 Yellow Cards"

	from players_stats ps
	where (
		(group_by_club AND ps.grouping_clubs != 'ALL')
   		OR (NOT group_by_club AND ps.grouping_clubs = 'ALL')
	)
	AND (
		(group_by_competition AND ps.grouping_competitions != 'ALL')
   		OR (NOT group_by_competition AND ps.grouping_competitions = 'ALL')
	)
	AND (
		(group_by_season AND ps.grouping_seasons != 'ALL')
   		OR (NOT group_by_season AND ps.grouping_seasons = 'ALL')
	);
end;
$$;
