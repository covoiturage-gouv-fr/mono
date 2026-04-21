{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    merge_update_columns=[
      'first_date_driver',
      'first_geo_code_driver',
      'first_date_passenger',
      'first_geo_code_passenger',
      'last_date_driver',
      'last_date_passenger',
      'driver_count',
      'passenger_count',
      'total_count'
    ],
    indexes=[
      { 'columns': ['user_id'], 'unique': true }
    ],
    tags=['refined', 'observatoire', 'users']
  )
}}

WITH journeys AS (
  SELECT
    driver_key AS user_id,
    start_datetime_tz::date AS driver_date,
    start_geo_code AS driver_geo_code,
    NULL::date AS passenger_date,
    NULL::varchar AS passenger_geo_code,
    1::int AS driver_count,
    0::int AS passenger_count
  FROM {{ ref('trusted_journeys') }}
  WHERE {{ time_filter('start_datetime_tz', type='date', default_start="'2020-01-01'") }}
    AND driver_key IS NOT NULL
    AND valid_acquisition_status = true
  UNION ALL

  SELECT
    passenger_key AS user_id,
    NULL::date AS driver_date,
    NULL::varchar AS driver_geo_code,
    start_datetime_tz::date AS passenger_date,
    start_geo_code AS passenger_geo_code,
    0::int AS driver_count,
    1::int AS passenger_count
  FROM {{ ref('trusted_journeys') }}
  WHERE {{ time_filter('start_datetime_tz', type='date', default_start="'2020-01-01'") }}
    AND passenger_key IS NOT NULL
    AND valid_acquisition_status = true
),

agg AS (
  SELECT
    user_id,
    MIN(driver_date) AS first_date_driver,
    (ARRAY_AGG(driver_geo_code ORDER BY driver_date ASC) FILTER (WHERE driver_geo_code IS NOT NULL))[1] AS first_geo_code_driver,
    MIN(passenger_date) AS first_date_passenger,
    (ARRAY_AGG(passenger_geo_code ORDER BY passenger_date ASC) FILTER (WHERE passenger_geo_code IS NOT NULL))[1] AS first_geo_code_passenger,
    MAX(driver_date) AS last_date_driver,
    MAX(passenger_date) AS last_date_passenger,
    SUM(driver_count) AS driver_count,
    SUM(passenger_count) AS passenger_count,
    SUM(driver_count + passenger_count) AS total_count
  FROM journeys
  GROUP BY user_id
)

SELECT
  a.user_id::varchar,
  {% if is_incremental() %}
  NULLIF(
    LEAST(
      COALESCE(t.first_date_driver, '9999-12-31'::date),
      COALESCE(a.first_date_driver, '9999-12-31'::date)
    ),
    '9999-12-31'::date
  )::date AS first_date_driver,
  CASE
    WHEN a.first_date_driver < t.first_date_driver
      THEN a.first_geo_code_driver
    ELSE t.first_geo_code_driver
  END AS first_geo_code_driver,
  NULLIF(
    LEAST(
      COALESCE(t.first_date_passenger, '9999-12-31'::date),
      COALESCE(a.first_date_passenger, '9999-12-31'::date)
    ),
    '9999-12-31'::date
  )::date AS first_date_passenger,
  CASE
    WHEN a.first_date_passenger < t.first_date_passenger
      THEN a.first_geo_code_passenger
    ELSE t.first_geo_code_passenger
  END AS first_geo_code_passenger,
  GREATEST(
    COALESCE(t.last_date_driver, '1970-01-01'::date),
    COALESCE(a.last_date_driver, '1970-01-01'::date)
  )::date AS last_date_driver,
  GREATEST(
    COALESCE(t.last_date_passenger, '1970-01-01'::date),
    COALESCE(a.last_date_passenger, '1970-01-01'::date)
  )::date AS last_date_passenger,
  (COALESCE(t.driver_count, 0) + a.driver_count)::integer AS driver_count,
  (COALESCE(t.passenger_count, 0) + a.passenger_count)::integer AS passenger_count,
  (COALESCE(t.total_count, 0) + a.total_count)::integer AS total_count
  FROM agg a
  LEFT JOIN {{ this }} t ON t.user_id = a.user_id
  {% else %}
  a.first_date_driver::date,
  a.first_geo_code_driver,
  a.first_date_passenger::date,
  a.first_geo_code_passenger,
  a.last_date_driver::date,
  a.last_date_passenger::date,
  a.driver_count::integer,
  a.passenger_count::integer,
  a.total_count::integer
  FROM agg a
  {% endif %}