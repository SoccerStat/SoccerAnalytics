drop function if exists analytics.one_teams_ranking;
create function analytics.one_teams_ranking(in_ranking character varying, in_comp character varying, in_seasons character varying[], first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Competition" character varying, "Ranking" character varying, "Club" character varying, "Stat" numeric)
    language plpgsql
as
$$
begin
	PERFORM analytics.check_teams_ranking(in_ranking);
	PERFORM analytics.check_side(side);

	RETURN QUERY
		with cup_ranking as (
			select * from analytics.cup_ranking(in_comp, in_seasons)
		),
		stats as (
			SELECT
				stats.competition as "Competition",
				stats.id_team,
				stats.club as "Club",

				analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",

				case
					when chp.id is not null
					then analytics.set_bigint_stat(sum(home_points), sum(away_points), side)
					else null
				end as "Points",

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

					WHEN 'Wins'                    THEN analytics.set_bigint_stat(sum(home_win), sum(away_win), side)
					WHEN 'Draws'                   THEN analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side)
					WHEN 'Loses'                   THEN analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side)

					-- WHEN 'Goals For'               THEN analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side)
					-- WHEN 'Goals Against'           THEN analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side)

					WHEN 'Clean Sheets'            THEN analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side)

					WHEN 'xG For'                  THEN analytics.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side)
					WHEN 'xG Against'              THEN analytics.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side)

					WHEN 'Yellow Cards'            THEN analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), side)
					WHEN 'Red Cards'               THEN analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side)
					WHEN 'Incl. 2 Yellow Cards'    THEN analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side)

					WHEN 'Fouls'                   THEN analytics.set_bigint_stat(sum(home_fouls), sum(away_fouls), side)

					ELSE NULL
				END::numeric as "Stat"

			FROM analytics.staging_teams_performance as "stats"
			LEFT JOIN upper.championship chp
			ON stats.id_comp = chp.id
			left join cup_ranking cr
			on stats.id_team = cr.id_team
			WHERE CASE WHEN lower(in_comp) LIKE '%all%' AND lower(in_comp) NOT LIKE '%bundesliga%' THEN true ELSE stats.competition = in_comp END
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

			GROUP BY stats.id_team, stats.competition, stats.club, chp.id
		)
		select
			stats."Competition",

			coalesce(
				cr.ranking,
				rank() over(
					partition by stats."Competition"
					order by
						stats."Points" desc,
						stats."Goals For" - stats."Goals Against" desc,
						stats."Goals For" desc
				)::text
			) as "Ranking",

			stats."Club",

			coalesce(
				stats."Stat",
				case in_ranking
					WHEN 'Matches'       THEN stats."Matches"
					WHEN 'Points'        THEN stats."Points"

					WHEN 'Goals For'     THEN stats."Goals For"
					WHEN 'Goals Against' THEN stats."Goals Against"

					WHEN 'Succ Passes' THEN stats."Succ Passes"
					WHEN 'Att Passes'  THEN stats."Att Passes"

					WHEN 'Shots For'     THEN stats."Shots For"
					WHEN 'Shots Against' THEN stats."Shots Against"

					WHEN 'Shots on Target For'     THEN stats."Shots on Target For"
					WHEN 'Shots on Target Against' THEN stats."Shots on Target Against"

					WHEN 'Points/Match'  THEN case when stats."Matches" = 0 then 0.0 else round(stats."Points"::numeric / stats."Matches"::numeric, r) end

					WHEN 'Goals Diff'    THEN stats."Goals For" - stats."Goals Against"

					WHEN 'Shots/onTarget Conversion Rate For'        THEN case when stats."Shots on Target For" = 0 then 0.0 else     round(stats."Shots For"::numeric / stats."Shots on Target For"::numeric, r) end
                    WHEN 'Shots/onTarget Conversion Rate Against'    THEN case when stats."Shots on Target Against" = 0 then 0.0 else round(stats."Shots Against"::numeric / stats."Shots on Target Against"::numeric, r) end
                    WHEN 'Shots/Goals Conversion Rate For'           THEN case when stats."Goals For" = 0 then 0.0 else               round(stats."Shots For"::numeric / stats."Goals For"::numeric, r) end
                    WHEN 'Shots/Goals Conversion Rate Against'       THEN case when stats."Goals Against" = 0 then 0.0 else           round(stats."Shots Against"::numeric / stats."Goals Against"::numeric, r) end
                    WHEN 'onTarget/Goals Conversion Rate For'        THEN case when stats."Goals For" = 0 then 0.0 else               round(stats."Shots on Target For"::numeric / stats."Goals For"::numeric, r) end
                    WHEN 'onTarget/Goals Conversion Rate Against'    THEN case when stats."Goals Against" = 0 then 0.0 else           round(stats."Shots on Target Against"::numeric / stats."Goals Against"::numeric, r) end

					WHEN 'Succ Passes Rate' THEN case when stats."Att Passes" = 0 then 0.0 else round(stats."Succ Passes"::numeric / stats."Att Passes"::numeric, r) end

					-- WHEN 'xG For/Match'     THEN round(stats."xG For" / stats."Matches"::numeric, r)
					-- WHEN 'xG Against/Match' THEN round(stats."xG Against" / stats."Matches"::numeric, r)
				end
			)::numeric as "Stat"
		from stats
		left join cup_ranking cr
		on stats.id_team = cr.id_team;
end;
$$;
