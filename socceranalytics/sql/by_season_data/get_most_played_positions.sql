with players_performance as (
    select
        id_team,
        id_player,
        array_cat(home_positions, away_positions) as positions
    from analytics.staging_players_performance
    where season = '{season}'
),
players_positions as (
	select id_team, id_player, unnest(positions) as positions
	from players_performance
),
positions_freq as (
	select id_team, id_player, positions, count(*) as freq
	from players_positions
	group by id_team, id_player, positions
	order by id_player, freq desc
),
most_played_positions as (
	select id_team, id_player, positions
	from (
		select
			id_team,
			id_player,
			positions,
			freq,
			ROW_NUMBER() OVER (PARTITION BY id_team, id_player ORDER BY freq DESC) AS rnk
		from positions_freq
	) ppe
	where rnk <= 2
)
UPDATE season_{season}.team_player tp
SET tp.positions = p.positions
FROM (
    select
        id_team,
        id_player,
        array_agg(distinct positions) as positions
    from most_played_positions
    group by id_team, id_player
) p
WHERE tp.team = p.id_team AND tp.player = p.id_player;