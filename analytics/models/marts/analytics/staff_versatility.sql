{{ config(materialized='table') }}

SELECT
    s.staff_id,
    s.name,
    COUNT(DISTINCT sr.role) AS distinct_roles_count,
    COUNT(DISTINCT sr.anime_id) AS anime_count
FROM {{ ref('stg_staff') }} s
JOIN {{ ref('stg_staff_roles') }} sr ON s.staff_id = sr.staff_id
GROUP BY s.staff_id, s.name
ORDER BY distinct_roles_count DESC, anime_count DESC
