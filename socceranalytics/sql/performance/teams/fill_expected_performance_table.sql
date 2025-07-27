insert into analytics.staging_teams_expected_performance
select
    se.id,
    se.match,
    e.outcome,
    e.played_home,
    se.xg_shot
from season_2024_2025.event e
join season_2024_2025.shot_event se
on e.id = se.id
where e.match = 'bc46a367'
order by played_home
;