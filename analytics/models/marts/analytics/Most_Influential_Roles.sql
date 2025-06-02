{{ config(materialized='table') }}

WITH role_ratings AS (
    SELECT
        r.role,
        AVG(ar.rating) AS avg_rating,
        COUNT(DISTINCT r.anime_id) AS project_count
    FROM {{ ref('stg_staff_roles') }} r
    JOIN {{ ref('stg_anime_ratings') }} ar ON r.anime_id = ar.anime_id
    GROUP BY r.role
),

overall_avg AS (
    SELECT AVG(rating) AS avg_rating FROM {{ ref('stg_anime_ratings') }}
)

SELECT
    role,
    project_count,
    avg_rating,
    avg_rating - (SELECT avg_rating FROM overall_avg) AS rating_impact
FROM role_ratings
WHERE project_count > 5
ORDER BY rating_impact DESC
LIMIT 15
