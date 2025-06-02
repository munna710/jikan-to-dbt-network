{{ config(materialized='view') }}

SELECT
    anime_id,
    staff_id ,
    role,
    created_at,
    updated_at
FROM {{ source('postgres', 'staff_roles') }}
