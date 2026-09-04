{{ config(severity='error', tags=['trusted', 'geo', 'perimeters']) }}

-- Clé métier = (year, arr, l_arr) : (year, arr) n'est pas unique sur l'étranger
-- 2021-2023. Un doublon = jointure cubée, qui explose au prochain reseed.
SELECT
  year,
  arr,
  l_arr,
  COUNT(*) AS n
FROM {{ ref('perimeters') }}
GROUP BY year, arr, l_arr
HAVING COUNT(*) > 1
