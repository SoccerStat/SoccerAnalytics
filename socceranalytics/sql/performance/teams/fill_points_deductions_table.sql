WITH points_deductions AS (
    SELECT
        '{season}' AS season,
        competition AS id_comp,
        chp.name AS competition,
        t.id AS id_team,
        c.name AS club,
        points_deductions
    FROM season_2021_2022.team t
    LEFT JOIN upper.championship chp
    on t.competition = chp.id
    LEFT JOIN upper.continental_cup c_cup
        on t.competition = c_cup.id
    LEFT JOIN upper.club c
        on t.club = c.id
    WHERE points_deductions IS NOT NULL
)
INSERT INTO analytics.staging_teams_points_deductions
SELECT * FROM points_deductions;