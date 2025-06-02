{{ config(materialized='view') }}

SELECT
    staff_id,
    name,
    created_at,
    updated_at
FROM {{ source('postgres', 'staff') }}

