INSERT INTO analytics.all_teams
SELECT distinct c.name as "Club"
FROM analytics.staging_teams_performance stp
LEFT JOIN UPPER.club c
ON stp.id_team = stp.id_comp || '_' || c.id
WHERE id_comp = '{name_comp}'
AND season = '{season}';