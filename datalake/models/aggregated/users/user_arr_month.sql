{#
  Présence mensuelle des utilisateurs par arrondissement et côté du trajet (start / end),
  source des unique_drivers / unique_passengers des grains longs de territory_* (rollup_uniques).
  Quirk : bâti sur le périmètre arr, il ignore les trajets géocodés sur un code commune PLM
  (75056, 69123, 13055), que plm et les périmètres supra comptent depuis les trajets.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['arr', 'incremental_date', 'user_id', 'role', 'side'],
    indexes=[
      { 'columns': ['arr', 'incremental_date', 'user_id', 'role', 'side'], 'unique': true },
      { 'columns': ['incremental_date'] }
    ],
    tags=['aggregated', 'users', 'month', 'daily']
  )
}}

{% set lb = grain_lookback('month') %}

WITH filtered_carpools AS (
  {{ filtered_carpools(
    'arr', lookback_nb=lb.nb, lookback_unit=lb.unit, with_new_users=false
  ) }}
),

sides AS (
  SELECT
    start_code    AS arr,
    'start'::text AS side,
    carpool_datetime,
    driver_key,
    passenger_key
  FROM filtered_carpools
  UNION ALL
  SELECT
    end_code    AS arr,
    'end'::text AS side,
    carpool_datetime,
    driver_key,
    passenger_key
  FROM filtered_carpools
),

presence AS (
  SELECT
    driver_key     AS user_id,
    'driver'::text AS role,  -- noqa: RF04
    arr,
    side,
    carpool_datetime
  FROM sides
  UNION ALL
  SELECT
    passenger_key     AS user_id,
    'passenger'::text AS role,  -- noqa: RF04
    arr,
    side,
    carpool_datetime
  FROM sides
)

SELECT
  arr,
  user_id,
  role,  -- noqa: RF04
  side,
  {{ incremental_columns('carpool_datetime', 'month') }}
FROM presence
WHERE user_id IS NOT NULL AND arr IS NOT NULL
GROUP BY 1, 2, 3, 4, {{ group_by_grain('month', 5) }}
