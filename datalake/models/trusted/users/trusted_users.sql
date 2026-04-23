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
      'driver_carpool_count',
      'passenger_carpool_count',
      'total_carpool_count',
      'driver_trip_count',
      'passenger_trip_count'
    ],
    indexes=[
      { 'columns': ['user_id'], 'unique': true }
    ],
    tags=['refined', 'observatoire', 'users']
  )
}}

WITH carpools AS (
  SELECT
    _id,
    operator_trip_id, 
    driver_key AS user_id,
    start_datetime_tz::date AS driver_date,
    start_geo_code AS driver_geo_code,
    NULL::date AS passenger_date,
    NULL::varchar AS passenger_geo_code,
    1::int AS driver_carpool_count,
    0::int AS passenger_carpool_count
  FROM {{ ref('trusted_carpools') }}
  WHERE {{ time_filter('start_datetime_tz', type='date', default_start="'2020-01-01'") }}
    AND driver_key IS NOT NULL
    AND valid_acquisition_status = true
  UNION ALL
  SELECT
    _id,
    operator_trip_id,
    passenger_key AS user_id,
    NULL::date AS driver_date,
    NULL::varchar AS driver_geo_code,
    start_datetime_tz::date AS passenger_date,
    start_geo_code AS passenger_geo_code,
    0::int AS driver_carpool_count,
    1::int AS passenger_carpool_count
  FROM {{ ref('trusted_carpools') }}
  WHERE {{ time_filter('start_datetime_tz', type='date', default_start="'2020-01-01'") }}
    AND passenger_key IS NOT NULL
    AND valid_acquisition_status = true
),

agg_carpool AS (
  SELECT
    user_id,
    MIN(driver_date) AS first_date_driver,
    (ARRAY_AGG(driver_geo_code ORDER BY driver_date ASC) FILTER (WHERE driver_geo_code IS NOT NULL))[1] AS first_geo_code_driver,
    MIN(passenger_date) AS first_date_passenger,
    (ARRAY_AGG(passenger_geo_code ORDER BY passenger_date ASC) FILTER (WHERE passenger_geo_code IS NOT NULL))[1] AS first_geo_code_passenger,
    MAX(driver_date) AS last_date_driver,
    MAX(passenger_date) AS last_date_passenger,
    SUM(driver_carpool_count) AS driver_carpool_count,
    SUM(passenger_carpool_count) AS passenger_carpool_count,
    SUM(driver_carpool_count + passenger_carpool_count) AS total_carpool_count
  FROM carpools
  GROUP BY user_id
),
agg_trip AS (
  SELECT
    user_id,
    COUNT(DISTINCT operator_trip_id) FILTER (WHERE passenger_geo_code IS NULL) AS driver_trip_count,
    COUNT(DISTINCT operator_trip_id) FILTER (WHERE driver_geo_code IS NULL) AS passenger_trip_count
  FROM carpools
  GROUP BY user_id
)

SELECT
  c.user_id::varchar,
  {% if is_incremental() %}
  NULLIF(
    LEAST(
      COALESCE(b.first_date_driver, '9999-12-31'::date),
      COALESCE(c.first_date_driver, '9999-12-31'::date)
    ),
    '9999-12-31'::date
  )::date AS first_date_driver,
  CASE
    WHEN c.first_date_driver < b.first_date_driver
      THEN c.first_geo_code_driver
    ELSE b.first_geo_code_driver
  END AS first_geo_code_driver,
  NULLIF(
    LEAST(
      COALESCE(b.first_date_passenger, '9999-12-31'::date),
      COALESCE(c.first_date_passenger, '9999-12-31'::date)
    ),
    '9999-12-31'::date
  )::date AS first_date_passenger,
  CASE
    WHEN c.first_date_passenger < b.first_date_passenger
      THEN c.first_geo_code_passenger
    ELSE b.first_geo_code_passenger
  END AS first_geo_code_passenger,
  GREATEST(
    COALESCE(b.last_date_driver, '1970-01-01'::date),
    COALESCE(c.last_date_driver, '1970-01-01'::date)
  )::date AS last_date_driver,
  GREATEST(
    COALESCE(b.last_date_passenger, '1970-01-01'::date),
    COALESCE(c.last_date_passenger, '1970-01-01'::date)
  )::date AS last_date_passenger,
  (COALESCE(b.driver_carpool_count, 0) + c.driver_carpool_count)::integer AS driver_carpool_count,
  (COALESCE(b.passenger_carpool_count, 0) + c.passenger_carpool_count)::integer AS passenger_carpool_count,
  (COALESCE(b.total_carpool_count, 0) + c.total_carpool_count)::integer AS total_carpool_count,
  (COALESCE(b.driver_trip_count, 0) + t.driver_trip_count)::integer AS driver_trip_count,
  (COALESCE(b.passenger_trip_count, 0) + t.passenger_trip_count)::integer AS passenger_trip_count
  FROM agg_carpool c
  LEFT JOIN agg_trip t ON t.user_id = c.user_id
  LEFT JOIN {{ this }} b ON b.user_id = c.user_id
  {% else %}
  c.first_date_driver::date,
  c.first_geo_code_driver,
  c.first_date_passenger::date,
  c.first_geo_code_passenger,
  c.last_date_driver::date,
  c.last_date_passenger::date,
  c.driver_carpool_count::integer,
  c.passenger_carpool_count::integer,
  c.total_carpool_count::integer,
  t.driver_trip_count::integer,
  t.passenger_trip_count::integer
  FROM agg_carpool c
  LEFT JOIN agg_trip t ON t.user_id = c.user_id
  {% endif %}