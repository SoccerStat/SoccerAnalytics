INSERT INTO analytics.all_teams
SELECT distinct club as "Club", club_country as "Country"
FROM analytics.staging_teams_performance
WHERE id_comp = '{id_comp}'
AND season = '{season}'
ON CONFLICT DO NOTHING;