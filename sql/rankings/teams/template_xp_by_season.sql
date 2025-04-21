insert into analytics.staging_teams_expected_performance
select 
    ts.match, 
    c.name as "Club", 
    ts.played_home, 
    ts.xg::numeric, 
    ts.nb_shots_total::bigint
from season_{season}.team_stats ts 
join season_{season}.match m on m.id = ts.match 
join upper.club c on ts.team = m.competition || '_' || c.id
where competition = '{id_comp}'