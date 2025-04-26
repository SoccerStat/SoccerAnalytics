create or replace function analytics.teams_ranking(
	in seasons varchar[],
	in comp varchar(20),
	in first_week int,
	in last_week int,
	in first_date varchar(20),
	in last_date varchar(20),
	in side analytics.side,
	in r int
)
returns table(
	"Ranking" bigint,
	"Club" varchar(100),
	"Attendance" numeric,
	"Matches" bigint,
	"Points" bigint,
	"Points/Match" numeric,
	"Wins" bigint,
	"Draws" bigint,
	"Loses" bigint,
	"Goals For" bigint,
	"Goals Against" bigint,
	"Goals Diff" bigint,
	"Clean Sheets" bigint,
	"xG For" numeric,
	"xG For /Match" numeric,
	"xG Against" numeric,
	"xG Against /Match" numeric,
	"Yellow Cards" bigint,
	"Red Cards" bigint,
	"Incl. 2 Yellow Cards" bigint,
	"Fouls" bigint,
	"Shots For" bigint,
	"Shots on Target For" bigint,
	"Shots Against" bigint,
	"Shots on Target Against" bigint--,
	--"Last Opponent" varchar(100)
)
as $$
DECLARE
	query text;
begin
	PERFORM analytics.check_side(side);

    ----,--get_last_opponent(c.id, id_season) as "Last Opponent"

	query := '
		with teams_stats as (
			select
				c.name as Club,
				
				round(avg(att), 0) as Attendance,

				sum(home_score) as home_score,
				sum(away_score) as away_score,
				
				analytics.set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,
				
				analytics.set_bigint_stat(sum(home_points), sum(away_points), ''' || side || ''') as Points,
				
				analytics.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
				analytics.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
				analytics.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,
				
				analytics.set_bigint_stat(sum(home_goals_for), sum(away_goals_for), ''' || side || ''') as "Goals For",
				analytics.set_bigint_stat(sum(home_goals_against), sum(away_goals_against), ''' || side || ''') as "Goals Against",
				analytics.set_bigint_stat(sum(home_goals_for - home_goals_against), sum(away_goals_for - away_goals_against), ''' || side || ''') as "Goals Diff",

				analytics.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), ''' || side || ''') as "Clean Sheets",
				
				analytics.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, ''' || side || ''') as "xG For",

				analytics.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, ''' || side || ''') as "xG Against",

				analytics.set_bigint_stat(sum(home_y_cards), sum(away_y_cards), ''' || side || ''') as "Yellow Cards",
				analytics.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards",
				analytics.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
				
				analytics.set_bigint_stat(sum(home_fouls), sum(away_fouls), ''' || side || ''') as Fouls,

				analytics.set_bigint_stat(sum(home_shots_for), sum(away_shots_for), ''' || side || ''') as "Shots For",
				analytics.set_bigint_stat(sum(home_shots_ot_for), sum(away_shots_ot_for), ''' || side || ''') as "Shots on Target For",

				analytics.set_bigint_stat(sum(home_shots_against), sum(away_shots_against), ''' || side || ''') as "Shots Against",
				analytics.set_bigint_stat(sum(home_shots_ot_against), sum(away_shots_ot_against), ''' || side || ''') as "Shots on Target Against"
				
			from analytics.staging_teams_performance as "stats"
			join (select id, name from upper.club) as c
			on stats.id_team = stats.id_comp || ''_'' || c.id
			left join upper.championship chp
			on stats.id_comp = chp.id
			where stats.season = any($1)
			and stats.competition = ''' || comp || '''
			and (
				(
					chp.id is not null
					and length(stats.week) <= 2 
					and cast(stats.week as int) between ''' || first_week || ''' and ''' || last_week || '''
				)
				or chp.id is null
			)
			and stats.date between ''' || first_date || '''::date and ''' || last_date || '''::date

			group by Club --, "Last Opponent"
		)

		select
			rank() over (
				order by ts.Points desc, ts."Goals Diff" desc
			) as Ranking,

			ts.Club,

			ts.Attendance,
			ts.Matches,

			ts.Points,
			round(ts.Points / ts.Matches::numeric, ''' || r || ''') as "Points/Match",

			ts.Wins,
			ts.Draws,
			ts.Loses,

			ts."Goals For",
			ts."Goals Against",
			ts."Goals Diff",

			ts."Clean Sheets",

			ts."xG For",
			case
				when ts.Matches <> 0 then
					round(ts."xG For" / ts.Matches::numeric, ''' || r || ''')
				else 0.0 
			end as "xG For /Match",

			ts."xG Against",
			case
				when ts.Matches <> 0 then
					round(ts."xG Against" / ts.Matches::numeric, ''' || r || ''')
				else 0.0
			end as "xG Against /Match",

			ts."Yellow Cards",
			ts."Red Cards",
			ts."Incl. 2 Yellow Cards",
			ts.Fouls,

			ts."Shots For",
			ts."Shots on Target For",
			
			ts."Shots Against",
			ts."Shots on Target Against"--,

			--ts."Last Opponent"
		from teams_stats ts;
	';

	RETURN QUERY EXECUTE query USING seasons;
		
end;
$$ language plpgsql;
