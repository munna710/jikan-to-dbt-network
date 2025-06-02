SELECT
  s.staff_id,
  s.name,
  r.role,
  COUNT(*) AS role_count,
  AVG(ar.rating) AS avg_rating,
  AVG(ar.rating) - overall.avg_rating AS rating_impact,
  -- Compare to their personal average across all roles
  AVG(ar.rating) - personal.avg_personal_rating AS role_specialization_score
FROM {{ ref('stg_staff_roles') }} r
JOIN {{ ref('stg_staff') }} s ON r.staff_id = s.staff_id
JOIN {{ ref('stg_anime_ratings') }} ar ON r.anime_id = ar.anime_id
JOIN (SELECT AVG(rating) AS avg_rating FROM {{ ref('stg_anime_ratings') }}) overall ON 1=1
JOIN (
  SELECT 
    staff_id, 
    AVG(rating) AS avg_personal_rating 
  FROM {{ ref('stg_staff_roles') }} r
  JOIN {{ ref('stg_anime_ratings') }} a ON r.anime_id = a.anime_id
  GROUP BY 1
) personal ON r.staff_id = personal.staff_id
GROUP BY 1, 2, 3, overall.avg_rating, personal.avg_personal_rating
HAVING COUNT(*) >= 3
ORDER BY role_specialization_score DESC