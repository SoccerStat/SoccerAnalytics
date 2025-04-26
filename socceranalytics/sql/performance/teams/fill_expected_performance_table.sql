insert into analytics.staging_teams_expected_performance
select
    '{season}' as season,
    coalesce(chp.name, c_cup.name) as comp,
    ts.match, 
    c.name as "Club", 
    ts.played_home,
    ts.xg::numeric, 
    ts.nb_shots_total::bigint as shots
from season_{season}.team_stats ts 
join season_{season}.match m on m.id = ts.match 
join upper.club c on ts.team = m.competition || '_' || c.id
left join upper.championship chp
on m.competition = chp.id
left join upper.continental_cup c_cup
on m.competition = c_cup.id
where competition = '{id_comp}';