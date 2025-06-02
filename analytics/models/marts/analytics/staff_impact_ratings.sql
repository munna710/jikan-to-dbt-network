{{ config(materialized='table') }}

SELECT
    s.staff_id,
    s.name,
    r.role,
    COUNT(DISTINCT r.anime_id) AS anime_count,
    AVG(ar.rating) AS avg_rating,
    AVG(ar.rating) - (SELECT AVG(rating) FROM {{ ref('stg_anime_ratings') }}) AS rating_impact,
    MIN(ar.rating) AS min_rating,
    MAX(ar.rating) AS max_rating
FROM {{ ref('stg_staff_roles') }} r
JOIN {{ ref('stg_staff') }} s ON r.staff_id = s.staff_id
JOIN {{ ref('stg_anime_ratings') }} ar ON r.anime_id = ar.anime_id
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT r.anime_id) >= 3  -- Only staff with sufficient work
ORDER BY rating_impact DESC