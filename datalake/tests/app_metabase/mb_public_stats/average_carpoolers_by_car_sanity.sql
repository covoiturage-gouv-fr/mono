-- Garde-fou de plausibilité : average_carpoolers_by_car =
-- SUM(passenger_seats)/SUM(carpools) + 1. Une valeur nulle, négative ou
-- étant grand devant des valeurs standard (3) trahit une régression dans
-- le plumbing operators/carpools/passenger_seats.
{{ config(severity='warn', tags=['app_metabase', 'public_stats']) }}

SELECT average_carpoolers_by_car
FROM {{ ref('mb_public_stats') }}
WHERE
  average_carpoolers_by_car IS NULL
  OR average_carpoolers_by_car <= 0
  OR average_carpoolers_by_car > 3
