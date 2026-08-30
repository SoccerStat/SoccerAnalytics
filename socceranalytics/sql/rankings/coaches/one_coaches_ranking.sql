drop function if exists analytics.one_coaches_ranking;

create function analytics.one_coaches_ranking(in_ranking character varying, in_comps character varying[], in_seasons character varying[], group_by_club boolean DEFAULT true, group_by_competition boolean DEFAULT true, group_by_season boolean DEFAULT true, first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Manager" character varying, "Competition" character varying[], "Club" character varying[], "Season" character varying[], "Ranking" character varying, "Matches" integer, "Involved Matches" integer, "Stat" numeric)
    language plpgsql
as
$$
DECLARE
	query text;
begin
	PERFORM analytics.check_coaches_ranking(in_ranking);
	PERFORM analytics.check_side(side);

	RETURN QUERY
		-- with cup_ranking as (
		-- 	select * from analytics.cup_ranking(in_comp, in_seasons)
		-- ),
		with coaches_stats as (
			SELECT
				case when played_home then home_manager else away_manager end as "Manager",
				array_agg(distinct stats.competition) as "Competition",
				array_agg(distinct stats.club) as "Club",
				array_agg(distinct stats.season) as "Season",

				case when grouping(stats.club) = 1 then 'ALL' else 'Single' end as grouping_clubs,
				case when grouping(stats.competition) = 1 then 'ALL' else 'Single' end as grouping_competitions,
				case when grouping(stats.season) = 1 then 'ALL' else 'Single' end as grouping_seasons,

				analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",
				analytics.set_bigint_stat(sum(home_points), sum(away_points), side) as "Points",

				case in_ranking
					when 'Wins'         then analytics.set_bigint_stat(sum(home_win), sum(away_win), side)
					when 'Draws'        then analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side)
					when 'Loses'        then analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side)
					when 'Clean Sheets' then analytics.set_bigint_stat(sum(case when home_clean_sheet != 0 then 1 else 0 end), sum(case when away_clean_sheet != 0 then 1 else 0 end), side)

					-- when 'Yellow Cards'         then analytics.set_bigint_stat(sum(case when home_y_cards != 0 then 1 else 0 end), sum(case when away_y_cards != 0 then 1 else 0 end), side)
					-- when 'Red Cards'            then analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side)
					-- when 'Incl. 2 Yellow Cards' then analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side)

					when 'Goals For'     then analytics.set_bigint_stat(sum(case when home_goals_for != 0 then 1 else 0 end), sum(case when away_goals_for != 0 then 1 else 0 end), side)
					when 'Goals Against' then analytics.set_bigint_stat(sum(case when home_goals_against != 0 then 1 else 0 end), sum(case when away_goals_against != 0 then 1 else 0 end), side)
					else null
				end as "Involved Matches",

				-- case
				--   when chp.id is not null
				--   then analytics.set_bigint_stat(sum(home_points), sum(away_points), side)
				--   else null
				-- end as "Points",

				analytics.set_bigint_stat(sum(home_win), sum(away_win), side) as "Wins",
				analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side) as "Loses",

				analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side) as "Goals For",
				analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side) as "Goals Against",

				analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), side) as "Shots For",
				analytics.set_bigint_stat(sum(home_shots_ot_for), sum(away_shots_ot_for), side) as "Shots on Target For",

				analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), side) as "Shots Against",
				analytics.set_bigint_stat(sum(home_shots_ot_against), sum(away_shots_ot_against), side) as "Shots on Target Against",

				analytics.set_bigint_stat(sum(home_goals_for - home_goals_against), sum(away_goals_for - away_goals_against), side) as "Goals Diff",

				analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes",
				analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",

				CASE in_ranking
					WHEN 'Minutes'                 THEN analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), side)
					WHEN 'Attendance'              THEN round(avg(att), 0)

					WHEN 'Draws'                   THEN analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side)

					WHEN 'Goals For'               THEN analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side)
					WHEN 'Goals Against'           THEN analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side)

					WHEN 'Clean Sheets'            THEN analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side)

					WHEN 'xG For'                  THEN analytics.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side)
					WHEN 'xG Against'              THEN analytics.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side)

					-- WHEN 'Yellow Cards'            THEN analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), side)
					-- WHEN 'Red Cards'               THEN analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side)
					-- WHEN 'Incl. 2 Yellow Cards'    THEN analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side)

					WHEN 'Fouls'                   THEN analytics.set_bigint_stat(sum(home_fouls), sum(away_fouls), side)

					ELSE NULL
				END::numeric as "Stat"

			FROM analytics.staging_teams_performance as "stats"
			LEFT JOIN upper.championship chp
			ON stats.id_comp = chp.id
			-- left join cup_ranking cr
			-- on stats.id_team = cr.id_team
			where CASE WHEN 'all' = ANY(ARRAY(SELECT lower(x) FROM unnest(in_comps) AS x)) and not 'bundesliga' = any(ARRAY(SELECT lower(x) FROM unnest(in_comps) AS x)) THEN true ELSE stats.competition = ANY(ARRAY(SELECT x FROM unnest(in_comps) AS x)) END
			AND stats.season = any(in_seasons)
			AND (
				(
					chp.id IS NOT NULL
					AND length(stats.week) <= 2
					AND cast(stats.week as int) BETWEEN first_week AND last_week
				)
				OR chp.id IS NULL
			)
			AND stats.date BETWEEN first_date::date AND last_date::date
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

			GROUP BY case when played_home then home_manager else away_manager end, cube(stats.competition, stats.club, stats.season)
		)
		select
			cs."Manager",
			cs."Competition",
			cs."Club",
			cs."Season",

			-- coalesce(
			--   null,
			--   -- cr.ranking,
			--   rank() over(
			--     partition by cs."Competition"
			--     order by
			--       cs."Points" desc,
			--       cs."Goals For" - cs."Goals Against" desc,
			--       cs."Goals For" desc
			--   )::text
			-- ) as "Ranking",

			rank() over(
				partition by cs."Competition"
				order by
					cs."Points" desc,
					cs."Goals For" - cs."Goals Against" desc,
					cs."Goals For" desc
			)::varchar as "Ranking",

			cs."Matches"::integer,
			cs."Involved Matches"::integer,

			coalesce(
				cs."Stat",
				case in_ranking
					WHEN 'Matches'       THEN cs."Matches"
					WHEN 'Points'        THEN cs."Points"

					WHEN 'Wins'  THEN cs."Wins"
					WHEN 'Loses' THEN cs."Loses"

					WHEN '% Wins'  THEN cs."Wins"::numeric / cs."Matches"
					WHEN '% Loses' THEN cs."Loses"::numeric / cs."Matches"

					WHEN 'Goals For'     THEN cs."Goals For"
					WHEN 'Goals Against' THEN cs."Goals Against"

					WHEN 'Succ Passes' THEN cs."Succ Passes"
					WHEN 'Att Passes'  THEN cs."Att Passes"

					WHEN 'Shots For'     THEN cs."Shots For"
					WHEN 'Shots Against' THEN cs."Shots Against"

					WHEN 'Shots on Target For'     THEN cs."Shots on Target For"
					WHEN 'Shots on Target Against' THEN cs."Shots on Target Against"

					WHEN 'Points/Match'  THEN case when cs."Matches" = 0 then 0.0 else round(cs."Points"::numeric / cs."Matches"::numeric, r) end

					WHEN 'Goals Diff'    THEN cs."Goals For" - cs."Goals Against"

					WHEN 'Shots/onTarget Conversion Rate For'        THEN case when cs."Shots on Target For" = 0 then 0.0 else     round(cs."Shots For"::numeric / cs."Shots on Target For"::numeric, r) end
                    WHEN 'Shots/onTarget Conversion Rate Against'    THEN case when cs."Shots on Target Against" = 0 then 0.0 else round(cs."Shots Against"::numeric / cs."Shots on Target Against"::numeric, r) end
                    WHEN 'Shots/Goals Conversion Rate For'           THEN case when cs."Goals For" = 0 then 0.0 else               round(cs."Shots For"::numeric / cs."Goals For"::numeric, r) end
                    WHEN 'Shots/Goals Conversion Rate Against'       THEN case when cs."Goals Against" = 0 then 0.0 else           round(cs."Shots Against"::numeric / cs."Goals Against"::numeric, r) end
                    WHEN 'onTarget/Goals Conversion Rate For'        THEN case when cs."Goals For" = 0 then 0.0 else               round(cs."Shots on Target For"::numeric / cs."Goals For"::numeric, r) end
                    WHEN 'onTarget/Goals Conversion Rate Against'    THEN case when cs."Goals Against" = 0 then 0.0 else           round(cs."Shots on Target Against"::numeric / cs."Goals Against"::numeric, r) end

					WHEN 'Succ Passes Rate' THEN case when cs."Att Passes" = 0 then 0.0 else round(cs."Succ Passes"::numeric / cs."Att Passes"::numeric, r) end

					-- WHEN 'xG For/Match'     THEN round(cs."xG For" / cs."Matches"::numeric, r)
					-- WHEN 'xG Against/Match' THEN round(cs."xG Against" / cs."Matches"::numeric, r)
				end
			)::numeric as "Stat"
		from coaches_stats cs
		-- left join cup_ranking cr
		-- on cs.id_team = cr.id_team
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
end;
$$;
