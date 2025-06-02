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
            WHEN role IN ('Character Design', 'Mechanical Design', 'Background Art', 'Color Design', 'Color Setting', 'Digital Paint', 'Layout', 'Storyboard', 'Special Effects', 'Editing', 'Art Director') THEN 'Design & Art'
            ELSE 'Other'
        END AS role_category
    FROM {{ ref('stg_staff_roles') }}
)

SELECT
    role_category,
    COUNT(DISTINCT anime_id) AS total_projects,
    COUNT(DISTINCT staff_id) AS unique_staff
FROM categorized_roles
GROUP BY role_category
ORDER BY total_projects DESC
