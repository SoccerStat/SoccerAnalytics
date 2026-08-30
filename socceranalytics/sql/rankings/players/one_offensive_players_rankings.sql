drop function if exists analytics.one_offensive_players_rankings;

create function analytics.one_offensive_players_rankings(in_ranking character varying, in_comp character varying, in_seasons character varying[], first_week integer DEFAULT 0, last_week integer DEFAULT 100, first_date character varying DEFAULT '1970-01-01'::character varying, last_date character varying DEFAULT '2099-12-31'::character varying, day_slots character varying[] DEFAULT '{}'::character varying[], time_slots character varying[] DEFAULT '{}'::character varying[], side analytics.side DEFAULT 'both'::analytics.side, r integer DEFAULT 2)
    returns TABLE("Player" character varying, "Stat" numeric, "Competition" character varying)
    language plpgsql
as
$$
DECLARE
	query text;
begin

-- Granularity Club
-- Si liste complète: 'TCC' (Tous Clubs Confondus)
-- Sinon : 'Club'

-- Granularity_competition
-- Si liste complète : 'TCC' (Toutes Compétitions Confondues)
-- Sinon : 'Competition'

	RETURN QUERY
	with players_nationalities as (
		select
			player,
			array_agg(distinct country) as Nationalities
		from upper.player_nationality pn
		group by player
	),
	players_stats as (
		select
			stats.id_player,
			pn.Nationalities,

			case
				when count(distinct c.name) > 1 and grouping(c.name) = 1
				then array_agg(distinct c.name)
				else array[c.name]
			end as Clubs,

			case
				when count(distinct stats.competition) > 1 and grouping(stats.competition) = 1
				then array_agg(distinct stats.competition)
				else array[stats.competition]
			end as Competitions,

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

			analytics.set_bigint_stat(sum(home_goals), sum(away_goals), side) as "Goals",
			analytics.set_bigint_stat(sum(home_pens_att), sum(away_pens_att), side) as "Att Penalties",
			analytics.set_bigint_stat(sum(home_pens_made), sum(away_pens_made), side) as "Succ Penalties",
			analytics.set_bigint_stat(sum(home_assists), sum(away_assists), side) as "Assists",

			analytics.set_numeric_stat(sum(home_xg)::numeric, sum(away_xg)::numeric, side) as "xG (fbref)",


			analytics.set_bigint_stat(sum(home_y_cards), sum(home_y_cards), side) as "Yellow Cards",
			analytics.set_bigint_stat(sum(home_r_cards), sum(home_r_cards), side) as "Red Cards",
			analytics.set_bigint_stat(sum(home_yr_cards), sum(home_yr_cards), side) as "Incl. 2 Yellow Cards",

			analytics.set_bigint_stat(sum(home_shots), sum(away_shots), side) as "Shots",
			analytics.set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), side) as "Shots on Target",

			analytics.set_bigint_stat(sum(home_passes_total), sum(away_passes_total), side) as "Att Passes",
			analytics.set_bigint_stat(sum(home_passes_succ), sum(away_passes_succ), side) as "Succ Passes",

			case
				when count(distinct c.name) = 1 and grouping(c.name) = 1
				then 'TCC'
				else 'Club'
			end as "Granularity Club",

			case
				when count(distinct stats.competition) = 1 and grouping(stats.competition) = 1
				then 'TCC'
				else 'Competition'
			end as "Granularity Competition"

		from analytics.staging_players_performance as "stats"
		join (select id, name from upper.club) as c
		on stats.id_team = stats.id_comp || '_' || c.id
		join players_nationalities pn
		on stats.id_player = pn.player || '_' || stats.id_team
		left join upper.championship chp
		on stats.id_comp = chp.id
		where CASE WHEN lower(in_comp) LIKE '%all%' AND lower(in_comp) NOT LIKE '%bundesliga%' THEN true ELSE stats.competition = in_comp END
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
			(stats.id_player, pn.Nationalities),
			cube(c.name, stats.competition)
		-- having (
		--   grouping(c.name) = 0 OR count(distinct c.name) > 1
		-- ) and (
		--   grouping(stats.competition) = 0 OR count(distinct stats.competition) > 1
		-- )
	)
	select
		p.name as Player,

		EXTRACT(YEAR FROM age(current_date, birth_date))::bigint AS Age,

		p.height::bigint as Height,
		p.weight::bigint as Weight,

		p.strong_foot as Footed,

		ps.Nationalities as Nationalities,

		ps.GK,

		ps.Clubs,

		ps.Competitions,

		ps.Matches,

		ps.Wins,
		ps.Draws,
		ps.Loses,

		ps.Goals,
		ps.Penalties,
		ps.Assists,

		ps.xG,
		case
			when ps.Matches <> 0 then round(ps.xG / ps.Matches, r)
			else 0.0
		end as "xG/90",

		ps."Clean Sheets",

		ps."Yellow Cards",
		ps."Red Cards",
		ps."Incl. 2 Yellow Cards",

		ps.Minutes,
		case
			when ps.Matches <> 0 then round(ps.Minutes / ps.Matches, r)
			else 0.0
		end as "Minutes/Match",

		ps.Captain,

		ps.Started,
		ps."Sub In",
		ps."Sub Out",

		ps."Granularity Club",
		ps."Granularity Competition"

	from players_stats ps
	join upper.player p
	on split_part(ps.id_player, '_', 1) = p.id;
end;
$$;
