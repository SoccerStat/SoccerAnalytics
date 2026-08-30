create function teams_oppositions(seasons character varying[], comps character varying[], team character varying, in_side analytics.side DEFAULT 'both'::analytics.side)
    returns TABLE("Season" character varying, "Team" character varying, "Opponent" character varying, "Team Country" character varying, "Opponent Country" character varying, "Competition" character varying, "Matches" bigint, "Wins" bigint, "Draws" bigint, "Loses" bigint, "Goals For" bigint, "Goals Against" bigint, "Shots For" bigint, "Shots on Target For" bigint, "Shots Against" bigint, "Shots on Target Against" bigint, "Yellow Cards" bigint, "Incl. 2 Yellow Cards" bigint, "Red Cards" bigint, "Granularity Season" text, "Granularity Competition" text)
    language plpgsql
as
$$
begin
	PERFORM analytics.check_side(in_side);

	RETURN QUERY
    select
        case
            when count(distinct season) > 1 and grouping(season) = 1
            then 'All'
            else season
        end as "Season",
        club as Team,
        opponent as Opponent,
        club_country as "Team Country",
        opponent_country as "Opponent Country",

        case
            when count(distinct competition) > 1 and grouping(competition) = 1
            then 'All'
            else competition
        end as Competition,

        analytics.set_bigint_stat(sum(home_match), sum(away_match), in_side) as Matches,
        analytics.set_bigint_stat(sum(home_win), sum(away_win), in_side) as Wins,
        analytics.set_bigint_stat(sum(home_draw), sum(away_draw), in_side) as Draws,
        analytics.set_bigint_stat(sum(home_lose), sum(away_lose), in_side) as Loses,

        analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), in_side) as "Goals For",
        analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), in_side) as "Goals Against",

        analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), in_side) as "Shots For",
        analytics.set_bigint_stat(sum(home_shots_ot_for), sum(away_shots_ot_for), in_side) as "Shots on Target For",

        analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), in_side) as "Shots Against",
        analytics.set_bigint_stat(sum(home_shots_ot_against), sum(away_shots_ot_against), in_side) as "Shots on Target Against",

        analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), in_side) as "Yellow Cards",
        analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), in_side) as "Incl. 2 Yellow Cards",
        analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), in_side) as "Red Cards",

        case
            when count(distinct season) > 1 and grouping(season) = 1
            then 'TSC'
            else 'Season'
        end as "Granularity Season",

        case
            when count(distinct competition) > 1 and grouping(competition) = 1
            then 'TCC'
            else 'Competition'
        end as "Granularity Competition"

    from analytics.staging_teams_performance tp
    where club = team
        and season = any(seasons)
        and competition = any(comps)
		and case
			when in_side = 'neutral' then round = 'Final'
			when in_side in ('home', 'away', 'both') then (round is null or round != 'Final')
			else true
		end
    group by grouping sets(
        (club, opponent, club_country, opponent_country, season, competition),
        (club, opponent, club_country, opponent_country, competition),
        (club, opponent, club_country, opponent_country)
    )
    having (grouping(competition) = 0 OR count(distinct competition) > 1) and (grouping(season) = 0 OR count(distinct season) > 1);

end;
$$;
