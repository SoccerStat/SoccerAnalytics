drop function if exists analytics.one_players_ranking;

create function analytics.one_players_ranking(in_ranking character varying, in_comps character varying[], in_seasons character varying[], group_by_club boolean DEFAULT true, group_by_competition boolean DEFAULT true, group_by_season boolean DEFAULT true, first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Player" character varying, "Club" character varying[], "Competition" character varying[], "Season" character varying[], "Matches" integer, "Involved Matches" integer, "Stat" numeric)
    language plpgsql
as
$$
DECLARE
	query text;
begin
	PERFORM analytics.check_offensive_players_ranking(in_ranking);
	PERFORM analytics.check_side(side);

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

			analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",
			case in_ranking
				when 'Minutes'      then analytics.set_bigint_stat(sum(case when home_minutes != 0 then 1 else 0 end), sum(case when away_minutes != 0 then 1 else 0 end), side)
				when 'Started'      then analytics.set_bigint_stat(sum(home_started), sum(away_started), side)
				when 'Sub In'       then analytics.set_bigint_stat(sum(home_sub_in), sum(away_sub_in), side)
				when 'Sub Out'      then analytics.set_bigint_stat(sum(home_sub_out), sum(away_sub_out), side)
				when 'Injured'      then analytics.set_bigint_stat(sum(home_injured), sum(away_injured), side)
				when 'Captain'      then analytics.set_bigint_stat(sum(home_captain), sum(away_captain), side)
				when 'Wins'         then analytics.set_bigint_stat(sum(home_win), sum(away_win), side)
				when 'Draws'        then analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side)
				when 'Loses'        then analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side)
				when 'Clean Sheets' then analytics.set_bigint_stat(sum(case when home_clean_sheet != 0 then 1 else 0 end), sum(case when away_clean_sheet != 0 then 1 else 0 end), side)

				when 'Att Penalties'  then analytics.set_bigint_stat(sum(case when home_pens_att != 0 then 1 else 0 end), sum(case when away_pens_att != 0 then 1 else 0 end), side)
				when 'Succ Penalties' then analytics.set_bigint_stat(sum(case when home_pens_made != 0 then 1 else 0 end), sum(case when away_pens_made != 0 then 1 else 0 end), side)

				when 'Yellow Cards'         then analytics.set_bigint_stat(sum(case when home_cards_yellow != 0 then 1 else 0 end), sum(case when away_cards_yellow != 0 then 1 else 0 end), side)
				when 'Red Cards'            then analytics.set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side)
				when 'Incl. 2 Yellow Cards' then analytics.set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side)

				when 'Goals'   then analytics.set_bigint_stat(sum(case when home_goals != 0 then 1 else 0 end), sum(case when away_goals != 0 then 1 else 0 end), side)
				when 'Assists' then analytics.set_bigint_stat(sum(case when home_assists != 0 then 1 else 0 end), sum(case when away_assists != 0 then 1 else 0 end), side)
				else null
			end as "Involved Matches",


			analytics.set_bigint_stat(sum(home_goals), sum(away_goals), side) as "Goals",
			analytics.set_bigint_stat(sum(home_assists), sum(away_assists), side) as "Assists",

			analytics.set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as "xG (fbref)",

			analytics.set_bigint_stat(sum(home_shots), sum(away_shots), side) as "Shots",
			analytics.set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), side) as "Shots on Target",

			analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",
			analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes",

			case in_ranking
				when 'Minutes'      then analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), side)
				when 'Started'      then analytics.set_bigint_stat(sum(home_started), sum(away_started), side)
				when 'Sub In'       then analytics.set_bigint_stat(sum(home_sub_in), sum(away_sub_in), side)
				when 'Sub Out'      then analytics.set_bigint_stat(sum(home_sub_out), sum(away_sub_out), side)
				when 'Injured'      then analytics.set_bigint_stat(sum(home_injured), sum(away_injured), side)
				when 'Captain'      then analytics.set_bigint_stat(sum(home_captain), sum(away_captain), side)
				when 'Wins'         then analytics.set_bigint_stat(sum(home_win), sum(away_win), side)
				when 'Draws'        then analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side)
				when 'Loses'        then analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side)
				when 'Clean Sheets' then analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side)

				when 'Att Penalties'  then analytics.set_bigint_stat(sum(home_pens_att), sum(away_pens_att), side)
				when 'Succ Penalties' then analytics.set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side)

				when 'Yellow Cards'         then analytics.set_bigint_stat(sum(home_cards_yellow), sum(away_cards_yellow), side)
				when 'Red Cards'            then analytics.set_bigint_stat(sum(home_cards_red), sum(away_cards_red), side)
				when 'Incl. 2 Yellow Cards' then analytics.set_bigint_stat(sum(home_cards_yellow_red), sum(away_cards_yellow_red), side)
				else null
			end::numeric as "Stat"

		from analytics.staging_players_performance as "stats"
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
		group by
			(split_part(stats.id_player, '_', 1), stats.player),
			cube(stats.club, stats.competition, stats.season)
		-- having (
		--   grouping(c.name) = 0 OR count(distinct c.name) > 1
		-- ) and (
		--   grouping(stats.competition) = 0 OR count(distinct stats.competition) > 1
		-- )
	)
	select
		ps."Player",
		ps."Club",
		ps."Competition",
		ps."Season",
		ps."Matches"::int,
		ps."Involved Matches"::int,

		coalesce(
			ps."Stat",
			case in_ranking
				when 'Matches'         then ps."Matches"
				when 'Goals'           then ps."Goals"
				when 'Assists'         then ps."Assists"
				when 'xG (fbref)'      then ps."xG (fbref)"
				when 'Shots'           then ps."Shots"
				when 'Shots on Target' then ps."Shots on Target"
				when 'Att Passes'      then ps."Att Passes"
				when 'Succ Passes'     then ps."Succ Passes"

				when 'xG (fbref) / Match'             then case when ps."Matches" <> 0 then round(ps."xG (fbref)"::numeric / ps."Matches", r) else 0.0 end
				when 'Succ Passes Rate'               then case when ps."Att Passes" <> 0 then round(ps."Succ Passes"::numeric / ps."Att Passes", r) else 0.0 end
				when 'Shots/onTarget Conversion Rate' then case when ps."Shots on Target" <> 0 then round(ps."Shots"::numeric / ps."Shots on Target", r) else 0.0 end
				when 'Shots/Goals Conversion Rate'    then case when ps."Goals" <> 0 then round(ps."Shots"::numeric / ps."Goals", r) else 0.0 end
				when 'onTarget/Goals Conversion Rate' then case when ps."Goals" <> 0 then round(ps."Shots on Target"::numeric / ps."Goals", r) else 0.0 end
				else null
			end
		) as "Stat"
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
