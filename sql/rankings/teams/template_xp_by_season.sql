insert into tmp_justice_ranking
select 
    ts.match, 
    c.name as "Club", 
    ts.played_home, 
    ts.xg::numeric, 
    ts.nb_shots_total::bigint
from dwh_{season}.team_stats ts 
join dwh_{season}.match m on m.id = ts.match 
join dwh_upper.club c on ts.team = m.competition || '_' || c.id
where
    m.competition = '{id_comp}' and 
    length(m.week) <= 2 and
    cast(m.week as int) >= '{first_week}' and
    cast(m.week as int) <= '{last_week}';