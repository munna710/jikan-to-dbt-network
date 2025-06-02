{{ config(materialized='view') }}

SELECT
    anime_id,
    rating,
    created_at,
    updated_at
FROM {{ source('postgres', 'anime_ratings') }}
