insert into tmp_justice_ranking
select 
    ts.match, 
    c.name as "Club", 
    ts.played_home, 
    ts.xg::numeric, 
    ts.nb_shots_total::bigint
from season_{season}.team_stats ts 
join season_{season}.match m on m.id = ts.match 
join upper.club c on ts.team = m.competition || '_' || c.id
where 
    competition = '{id_comp}' 
    and (
        cast(week as int) between '{first_week}' and '{last_week}' 
        and length(week) <= 2
        or m.competition not in (select c.id from upper.championship c)
    )
    and m.date between '{first_date}' and '{last_date}';;