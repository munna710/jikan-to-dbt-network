{{ config(materialized='view') }}

SELECT
    anime_id,
    title,
    created_at,
    updated_at
FROM {{ source('postgres', 'anime') }}

