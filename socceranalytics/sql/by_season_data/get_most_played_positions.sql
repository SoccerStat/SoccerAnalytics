with players_performance as (
    select
        id_team,
        id_player,
        array_cat(home_positions, away_positions) as positions
    from analytics.staging_players_performance
    where season = '{season}'
),
players_positions as (
    select id_team, id_player, unnest(positions) as position
    from players_performance
),
positions_freq as (
    select id_team, id_player, position, count(*) as freq
    from players_positions
    group by id_team, id_player, position
),
freq_totals as (
    select id_team, id_player, sum(freq) as total_freq
    from positions_freq
    group by id_team, id_player
),
positions_ordered as (
    select
        pf.id_team,
        pf.id_player,
        pf.position,
        pf.freq,
        ft.total_freq,
        sum(pf.freq) over (
            partition by pf.id_team, pf.id_player
            order by pf.freq desc, pf.position asc
            rows between unbounded preceding and current row
        ) as cum_freq,
        DENSE_RANK() over (
            partition by pf.id_team, pf.id_player
            order by pf.freq desc
        ) as rn
    from positions_freq pf
    join freq_totals ft on pf.id_team = ft.id_team and pf.id_player = ft.id_player
),
positions_ordered_enriched as (
	select *,
		cum_freq / total_freq as cum_freq_pct,
		lag(rn) over (partition by id_team, id_player order by freq desc, position asc) as prev_rn,
		lead(rn) over (partition by id_team, id_player order by freq desc, position asc) as next_rn
	from positions_ordered
),
positions_to_keep as (
	select *
	from positions_ordered_enriched
	where case
		when cum_freq_pct >= 0.8 and (rn = next_rn or rn = prev_rn or next_rn is null and rn != 1)
		then cum_freq_pct < 0.8
		when cum_freq_pct < 0.8 and (rn = next_rn or rn = prev_rn)
		then false
		else true
	end
)
update season_{season}.team_player tp
set positions = p.positions
from (
    select
        id_team,
        id_player,
        array_agg(distinct positions)::varchar[] as positions
    from positions_to_keep
    group by id_team, id_player
) p
where tp.team = p.id_team and tp.player = p.id_player;