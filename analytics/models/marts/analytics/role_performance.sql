{{ config(materialized='table') }}

SELECT
    r.role,
    COUNT(DISTINCT r.anime_id) AS anime_count,
    COUNT(DISTINCT r.staff_id) AS unique_staff,
    AVG(ar.rating) AS avg_rating,
    AVG(ar.rating) - (
        SELECT AVG(rating) FROM {{ ref('stg_anime_ratings') }}
    ) AS rating_impact
FROM {{ ref('stg_staff_roles') }} r
JOIN {{ ref('stg_anime_ratings') }} ar ON r.anime_id = ar.anime_id
GROUP BY 1
ORDER BY anime_count DESC
