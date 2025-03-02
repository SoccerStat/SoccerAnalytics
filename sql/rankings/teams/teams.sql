create or replace function dwh_utils.teams_ranking(
	in side dwh_utils.ranking_type,
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
	"Shots" bigint,
	"Shots on Target" bigint--,
	--"Last Opponent" varchar(100)
)
as $$
DECLARE
    season_schema text;
	query text;
begin
	PERFORM dwh_utils.check_side(side);

    ----,--get_last_opponent(c.id, id_season) as "Last Opponent"

	query := '
		with teams_stats as (
			select
				c.name as Club,
				
				round(avg(att), 0) as Attendance,

				sum(home_score) as home_score,
				sum(away_score) as away_score,
				
				dwh_utils.set_bigint_stat(sum(home_match), sum(away_match), ''' || side || ''') as Matches,
				
				dwh_utils.set_bigint_stat(sum(home_points), sum(away_points), ''' || side || ''') as Points,
				
				dwh_utils.set_bigint_stat(sum(home_win), sum(away_win), ''' || side || ''') as Wins,
				dwh_utils.set_bigint_stat(sum(home_draw), sum(away_draw), ''' || side || ''') as Draws,
				dwh_utils.set_bigint_stat(sum(home_lose), sum(away_lose), ''' || side || ''') as Loses,
				
				dwh_utils.set_bigint_stat(sum(home_goal_for), sum(away_goal_for), ''' || side || ''') as "Goals For",
				dwh_utils.set_bigint_stat(sum(home_goal_against), sum(away_goal_against), ''' || side || ''') as "Goals Against",
				dwh_utils.set_bigint_stat(sum(home_goal_for - home_goal_against), sum(away_goal_for - away_goal_against), ''' || side || ''') as "Goals Diff",

				dwh_utils.set_bigint_stat(sum(home_clean_sheet), sum(away_clean_sheet), ''' || side || ''') as "Clean Sheets",
				
				dwh_utils.set_numeric_stat(sum(home_xg_for)::numeric, sum(away_xg_for)::numeric, ''' || side || ''') as "xG For",

				dwh_utils.set_numeric_stat(sum(home_xg_against)::numeric, sum(away_xg_against)::numeric, ''' || side || ''') as "xG Against",

				dwh_utils.set_bigint_stat(sum(home_y_cards), sum(away_y_card), ''' || side || ''') as "Yellow Cards",
				dwh_utils.set_bigint_stat(sum(home_r_cards), sum(away_r_cards), ''' || side || ''') as "Red Cards",
				dwh_utils.set_bigint_stat(sum(home_yr_cards), sum(away_yr_cards), ''' || side || ''') as "Incl. 2 Yellow Cards",
				
				dwh_utils.set_bigint_stat(sum(home_fouls), sum(away_fouls), ''' || side || ''') as Fouls,

				dwh_utils.set_bigint_stat(sum(home_shots), sum(away_shots), ''' || side || ''') as Shots,
				dwh_utils.set_bigint_stat(sum(home_shots_ot), sum(away_shots_ot), ''' || side || ''') as "Shots on Target"
				
			from pg_temp.tmp_ranking as "stats"
			join (select id, name from dwh_upper.club) as c
			on team = competition || ''_'' || c.id
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

			ts.Shots,
			ts."Shots on Target"--,

			--ts."Last Opponent"
		from teams_stats ts;
		;
	';

	RETURN QUERY EXECUTE query USING side, r;
		
end;
$$ language plpgsql;
