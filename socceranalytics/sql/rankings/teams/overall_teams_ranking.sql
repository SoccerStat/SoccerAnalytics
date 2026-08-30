drop function if exists analytics.overall_teams_ranking;
create function analytics.overall_teams_ranking(in_comp character varying, in_seasons character varying[], first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side)
    returns TABLE("Ranking" character varying, "Club" character varying, "Matches" bigint, "Wins" bigint, "Draws" bigint, "Loses" bigint, "Goals For" bigint, "Goals Against" bigint, "Goals Diff" bigint, "Points" bigint)
    language plpgsql
as
$$
DECLARE
	query text;
begin
	PERFORM analytics.check_side(side);

	RETURN QUERY
		with ranking as (
			SELECT
				stats.club as "Club",

				analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as "Matches",

				analytics.set_bigint_stat(sum(home_win), sum(away_win), side) as "Wins",
				analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side) as "Draws",
				analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side) as "Loses",

				analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side) as "Goals For",
				analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side) as "Goals Against",
				analytics.set_bigint_stat(sum(home_goals_for - home_goals_against), sum(away_goals_for - away_goals_against), side) as "Goals Diff",

				analytics.set_bigint_stat(sum(home_points), sum(away_points), side) as "Points"

			FROM analytics.staging_teams_performance as "stats"
			WHERE CASE WHEN lower(in_comp) LIKE '%all%' AND lower(in_comp) NOT LIKE '%bundesliga%' THEN true ELSE stats.competition = in_comp END
			AND stats.season = any(in_seasons)
			AND (
				(
					stats.competition IS NOT NULL
					AND length(stats.week) <= 2
					AND cast(stats.week as int) BETWEEN first_week AND last_week
				)
				OR stats.competition IS NULL
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

			GROUP BY stats.club
		)
		SELECT
			rank() over(
				order by
					r."Points" desc,
					r."Goals Diff" desc,
					r."Goals For" desc
			)::varchar as "Ranking",
			*
		FROM ranking r
		ORDER BY r."Points" desc, r."Goals Diff" desc, r."Goals For" desc;
end;
$$;

alter function analytics.overall_teams_ranking(varchar, character varying[], integer, integer, varchar, varchar, character varying[], character varying[], analytics.side) owner to prd_analytics;
