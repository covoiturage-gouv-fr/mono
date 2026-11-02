{#
  Présence mensuelle des utilisateurs par arrondissement et côté du trajet (start / end),
  source des unique_drivers / unique_passengers des grains longs de territory_* (rollup_uniques).
  Bâti sur le périmètre 'geo' (sans le filtre de l'arr) : city_trip marque les trajets qu'il
  exclut, pour que la variante arr le réapplique.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['arr', 'incremental_date', 'user_id', 'role', 'side', 'city_trip'],
    indexes=[
      { 'columns': ['arr', 'incremental_date', 'user_id', 'role', 'side', 'city_trip'], 'unique': true },
      { 'columns': ['incremental_date'] }
    ],
    tags=['aggregated', 'users', 'month', 'daily']
  )
}}

{% set lb = grain_lookback('month') %}

WITH filtered_carpools AS (
  {{ filtered_carpools(
    'geo', lookback_nb=lb.nb, lookback_unit=lb.unit, with_new_users=false
  ) }}
),

sides AS (
  SELECT
    start_code    AS arr,
    'start'::text AS side,
    carpool_datetime,
    driver_key,
    passenger_key,
    (
      start_code IN ('75056', '13055', '69123')
      OR end_code IN ('75056', '13055', '69123')
    )             AS city_trip
  FROM filtered_carpools
  UNION ALL
  SELECT
    end_code    AS arr,
    'end'::text AS side,
    carpool_datetime,
    driver_key,
    passenger_key,
    (
      start_code IN ('75056', '13055', '69123')
      OR end_code IN ('75056', '13055', '69123')
    )           AS city_trip
  FROM filtered_carpools
),

presence AS (
  SELECT
    driver_key     AS user_id,
    'driver'::text AS role,  -- noqa: RF04
    arr,
    side,
    city_trip,
    carpool_datetime
  FROM sides
  UNION ALL
  SELECT
    passenger_key     AS user_id,
    'passenger'::text AS role,  -- noqa: RF04
    arr,
    side,
    city_trip,
    carpool_datetime
  FROM sides
)

SELECT
  arr,
  user_id,
  role,  -- noqa: RF04
  side,
  city_trip,
  {{ incremental_columns('carpool_datetime', 'month') }}
FROM presence
WHERE user_id IS NOT NULL AND arr IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, {{ group_by_grain('month', 6) }}
