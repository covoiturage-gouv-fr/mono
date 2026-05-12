{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['user_id', 'incremental_date', 'role', 'start_geo_code', 'end_geo_code'],
    indexes=[
      { 'columns': ['user_id'] },
      { 'columns': ['incremental_date'] },
      { 'columns': ['start_geo_code', 'end_geo_code'] }
    ],
    tags=['aggregated', 'users', 'od', 'daily']
  )
}}

WITH carpools AS (
  SELECT
    driver_key AS user_id,
    start_datetime_tz::date AS carpool_date,
    'driver' AS role,
    start_geo_code,
    end_geo_code
  FROM {{ ref('carpools') }}
  WHERE {{ time_filter('start_datetime_tz', 'incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=3) }}
    AND driver_key IS NOT NULL
    AND valid_acquisition_status = true
  UNION ALL
  SELECT
    passenger_key AS user_id,
    start_datetime_tz::date AS carpool_date,
    'passenger' AS role,
    start_geo_code,
    end_geo_code
  FROM {{ ref('carpools') }}
  WHERE {{ time_filter('start_datetime_tz', 'incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=3) }}
    AND passenger_key IS NOT NULL
    AND valid_acquisition_status = true
)

SELECT
  user_id,
  carpool_date AS incremental_date,
  role,
  start_geo_code,
  end_geo_code,
  COUNT(*) AS carpools
FROM carpools
GROUP BY 1, 2, 3, 4, 5