{{ config(materialized='table') }}

WITH collaborations AS (
    SELECT
        a.staff_id AS staff1_id,
        b.staff_id AS staff2_id,
        COUNT(DISTINCT a.anime_id) AS projects_together
    FROM {{ ref('stg_staff_roles') }} a
    JOIN {{ ref('stg_staff_roles') }} b 
        ON a.anime_id = b.anime_id 
        AND a.staff_id < b.staff_id  -- Avoid duplicate pairs
    GROUP BY 1, 2
)

SELECT 
    s1.name AS staff1_name,
    s2.name AS staff2_name,
    c.projects_together,
    RANK() OVER (ORDER BY c.projects_together DESC) AS collaboration_rank
FROM collaborations c
JOIN {{ ref('stg_staff') }} s1 ON c.staff1_id = s1.staff_id
JOIN {{ ref('stg_staff') }} s2 ON c.staff2_id = s2.staff_id
WHERE projects_together >= 3  -- Filter for significant collaborations
ORDER BY projects_together DESC