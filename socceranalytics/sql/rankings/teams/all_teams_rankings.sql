drop function if exists analytics.all_teams_rankings;
create function analytics.all_teams_rankings(in_comp character varying, in_seasons character varying[], first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Competition" character varying, "Ranking" character varying, "Club" character varying, "Attendance" numeric, "Matches" bigint, "Minutes" bigint, "Points" bigint, "Points/Match" numeric, "Wins" bigint, "Draws" bigint, "Loses" bigint, "Goals For" bigint, "Goals Against" bigint, "Goals Diff" bigint, "Clean Sheets" bigint, "xG For (fbref)" numeric, "xG For/Match (fbref)" numeric, "xG Against (fbref)" numeric, "xG Against/Match (fbref)" numeric, "Yellow Cards" bigint, "Red Cards" bigint, "Incl. 2 Yellow Cards" bigint, "Fouls" bigint, "Shots For" bigint, "Shots on Target For" bigint, "Shots Against" bigint, "Shots on Target Against" bigint, "Succ Passes" bigint, "Att Passes" bigint, "Shots/onTarget Conversion Rate For" numeric, "Shots/onTarget Conversion Rate Against" numeric, "Shots/Goals Conversion Rate For" numeric, "Shots/Goals Conversion Rate Against" numeric, "onTarget/Goals Conversion Rate For" numeric, "onTarget/Goals Conversion Rate Against" numeric, "Succ Passes Rate" numeric)
    language plpgsql
as
$$
begin
	PERFORM analytics.check_side(side);

	RETURN QUERY
	with cup_ranking as (
		select *
		from analytics.cup_ranking(in_comp, in_seasons)
	),
	teams_stats as (
		select
			stats.id_team,
			stats.club as "Club",
			stats.competition as "Competition",

			round(avg(att), 0) as Attendance,

			analytics.set_bigint_stat(sum(home_minutes), sum(away_minutes), side) as "Minutes",

			-- sum(home_score) as home_score,
			-- sum(away_score) as away_score,

			analytics.set_bigint_stat(sum(home_match), sum(away_match), side) as Matches,

			case
				when chp.id is not null
				then analytics.set_bigint_stat(sum(home_points), sum(away_points), side)
				else null
			end as Points,

			analytics.set_bigint_stat(sum(home_win), sum(away_win), side) as Wins,
			analytics.set_bigint_stat(sum(home_draw), sum(away_draw), side) as Draws,
			analytics.set_bigint_stat(sum(home_lose), sum(away_lose), side) as Loses,

			analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), side) as "Goals For",
			analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), side) as "Goals Against",
			analytics.set_bigint_stat(sum(home_goals_for - home_goals_against), sum(away_goals_for - away_goals_against), side) as "Goals Diff",

			analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), side) as "Clean Sheets",

			analytics.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, side) as "xG For",

			analytics.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, side) as "xG Against",

			analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), side) as "Yellow Cards",
			analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), side) as "Red Cards",
			analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), side) as "Incl. 2 Yellow Cards",

			analytics.set_bigint_stat(sum(home_fouls), sum(away_fouls), side) as Fouls,

			analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), side) as "Shots For",
			analytics.set_bigint_stat(sum(home_shots_ot_for), sum(away_shots_ot_for), side) as "Shots on Target For",

			analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes",
			analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",

			analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), side) as "Shots Against",
			analytics.set_bigint_stat(sum(home_shots_ot_against), sum(away_shots_ot_against), side) as "Shots on Target Against"

		from analytics.staging_teams_performance as stats
		left join upper.championship chp
		on stats.id_comp = chp.id
		WHERE CASE WHEN lower(in_comp) LIKE '%all%' AND lower(in_comp) NOT LIKE '%bundesliga%' THEN true ELSE stats.competition = in_comp END
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

		group by stats.id_team, stats.club, stats.competition, chp.id -- , "Last Opponent"
	)

	select
		ts."Competition",
		coalesce(
			cr.ranking,
			rank() over(partition by ts."Competition" order by ts.Points desc, ts."Goals Diff" desc, ts."Goals For" desc)::text
		) as "Ranking",

		ts."Club",

		ts.Attendance,
		ts.Matches,
		ts."Minutes",

		ts.Points,
		case
			when side = 'neutral'
			then null::numeric
			else case
				when ts.Matches != 0
				then round(ts.Points / ts.Matches::numeric, r)
				else null::numeric
			end
		end as "Points/Match",

		ts.Wins,
		ts.Draws,
		ts.Loses,

		ts."Goals For",
		ts."Goals Against",
		ts."Goals Diff",

		ts."Clean Sheets",

		ts."xG For" as "xG For (fbref)",
		case
			when ts.Matches != 0 then
				round(ts."xG For" / ts.Matches::numeric, r)
			else 0.0
		end as "xG For/Match (fbref)",

		ts."xG Against" as "xG Against (fbref)",
		case
			when ts.Matches != 0 then
				round(ts."xG Against" / ts.Matches::numeric, r)
			else 0.0
		end as "xG Against/Match (fbref)",

		ts."Yellow Cards",
		ts."Red Cards",
		ts."Incl. 2 Yellow Cards",
		ts.Fouls,

		ts."Shots For",
		ts."Shots on Target For",

		ts."Shots Against",
		ts."Shots on Target Against",

		ts."Succ Passes",
		ts."Att Passes",

		case when ts."Shots on Target For"     = 0 then 0.0 else round(ts."Shots For"::numeric / ts."Shots on Target For"::numeric, r)         end as "Shots/onTarget Conversion Rate For",
		case when ts."Shots on Target Against" = 0 then 0.0 else round(ts."Shots Against"::numeric / ts."Shots on Target Against"::numeric, r) end as "Shots/onTarget Conversion Rate Against",
		case when ts."Goals For"               = 0 then 0.0 else round(ts."Shots For"::numeric / ts."Goals For"::numeric, r)                   end as "Shots/Goals Conversion Rate For",
		case when ts."Goals Against"           = 0 then 0.0 else round(ts."Shots Against"::numeric / ts."Goals Against"::numeric, r)           end as "Shots/Goals Conversion Rate Against",
		case when ts."Goals For"               = 0 then 0.0 else round(ts."Shots on Target For"::numeric / ts."Goals For"::numeric, r)         end as "onTarget/Goals Conversion Rate For",
		case when ts."Goals Against"           = 0 then 0.0 else round(ts."Shots on Target Against"::numeric / ts."Goals Against"::numeric, r) end as "onTarget/Goals Conversion Rate Against",

		case when ts."Att Passes"              = 0 then 0.0 else round(ts."Succ Passes"::numeric / ts."Att Passes"::numeric, r)                end as "Succ Passes Rate"

		-- ts."Last Opponent"
	from teams_stats ts
	left join cup_ranking cr
	on ts.id_team = cr.id_team;

end;
$$;
