INSERT INTO analytics.mapping_clubs_soccerstat_understat
WITH best_match AS (
    SELECT DISTINCT ON (stp.id_team, stp.season, stp.competition)
        c.name AS "club_soccerstat",
        u1."Club" AS "club_understat"
    FROM (select distinct season, id_comp, competition, id_team from analytics.staging_teams_performance where competition  not like 'UEFA%') stp
    LEFT JOIN upper.club c
        ON stp.id_team = stp.id_comp || '_' || c.id
    LEFT JOIN (select distinct "Season", "Competition", "Club" from understat.staging_teams_understat_performance) u1
        ON stp.season = u1."Season"
        AND stp.competition = u1."Competition"
    WHERE (
          similarity(lower(unaccent(c.name)), lower(unaccent(u1."Club"))) > 0.3
          OR similarity(lower(unaccent(c.city)), lower(unaccent(u1."Club"))) > 0.3
      )
    ORDER BY stp.id_team, stp.season, stp.competition, similarity(unaccent(c.name), unaccent(u1."Club")) desc
)
select distinct *
from best_match;