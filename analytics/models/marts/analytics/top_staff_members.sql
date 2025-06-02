{{ config(materialized='table') }}

SELECT
    s.staff_id,
    s.name,
    COUNT(DISTINCT r.role) AS unique_roles,
    COUNT(DISTINCT r.anime_id) AS total_projects
FROM {{ ref('stg_staff_roles') }} r
JOIN {{ ref('stg_staff') }} s ON r.staff_id = s.staff_id
GROUP BY s.staff_id, s.name
ORDER BY unique_roles DESC, total_projects DESC
LIMIT 20
