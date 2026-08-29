INSERT INTO understat.staging_teams_understat_performance(
    "match",
    "competition",
    "season",
    "name_team",
    "name_opponent",
    "played_home",
    "home_xg_for",
    "away_xg_for",
    "home_xg_against",
    "away_xg_against"
)
VALUES ('{match}', '{competition}', '{season}', '{name_team}', '{name_opponent}', '{played_home}', '{home_xg_for}', '{away_xg_for}', '{home_xg_against}', '{away_xg_against}')
ON CONFLICT ("match", "played_home")
DO UPDATE SET
    "competition" = EXCLUDED."competition",
    "season" = EXCLUDED."season",
    "name_team" = EXCLUDED."name_team",
    "name_opponent" = EXCLUDED."name_opponent",
    "home_xg_for" = EXCLUDED."home_xg_for",
    "away_xg_for" = EXCLUDED."away_xg_for",
    "home_xg_against" = EXCLUDED."home_xg_against",
    "away_xg_against" = EXCLUDED."away_xg_against";