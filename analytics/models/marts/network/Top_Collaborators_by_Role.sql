{{ config(materialized='table') }}

WITH categorized_roles AS (
    SELECT
        staff_id,
        anime_id,
        CASE
            WHEN role IN ('Director', 'Co-Director', 'Assistant Director', 'Episode Director', 'Series Production Director') THEN 'Directing'
            WHEN role IN ('Chief Animation Director', 'Animation Director', '2nd Key Animation', 'Key Animation', 'Assistant Animation Director', 'Animation Check', 'In-Between Animation') THEN 'Animation'
            WHEN role IN ('Producer', 'Executive Producer', 'Co-Producer', 'Assistant Producer', 'Production Manager', 'Production Coordination', 'Planning Producer', 'Associate Producer') THEN 'Production'
            WHEN role IN ('Music', 'Sound Director', 'Sound Effects', 'Recording', 'Recording Engineer', 'ADR Director', 'Dialogue Editing', 'Theme Song Composition', 'Theme Song Arrangement', 'Theme Song Lyrics', 'Inserted Song Performance', 'Re-Recording Mixing') THEN 'Music & Sound'
            WHEN role IN ('Voice Actor', 'Seiyuu', 'Casting Director') THEN 'Voice Acting'
            WHEN role IN ('Character Design', 'Mechanical Design', 'Background Art', 'Color Design', 'Color Setting', 'Digital Paint', 'Layout', 'Storyboard', 'Special Effects', 'Editing', 'Art Director') THEN 'Design & Art'
            ELSE 'Other'
        END AS role_category
    FROM {{ ref('stg_staff_roles') }}
),

collaborations AS (
    SELECT
        a.staff_id AS staff1_id,
        b.staff_id AS staff2_id,
        a.role_category,
        COUNT(DISTINCT a.anime_id) AS projects_together
    FROM categorized_roles a
    JOIN categorized_roles b
        ON a.anime_id = b.anime_id
        AND a.staff_id < b.staff_id
        AND a.role_category = b.role_category
    GROUP BY staff1_id, staff2_id, a.role_category
)

SELECT
    role_category,
    staff1_id,
    staff2_id,
    projects_together
FROM collaborations
WHERE projects_together >= 3
ORDER BY role_category, projects_together DESC
LIMIT 20
