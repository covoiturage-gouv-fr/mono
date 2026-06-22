{{
  config(
    materialized='table',
    indexes=[
      { 'columns': ['user_id'], 'unique': true },
      { 'columns': ['carpool_v2_id'] }
    ],
    tags=['aggregated', 'cee', 'primo_drivers']
  )
}}

WITH cee_journeys AS (
  SELECT
    j._id                     AS carpool_v2_id,
    j.driver_key,
    j.start_datetime_tz::date AS start_date
  FROM {{ ref('carpools') }} AS j
  INNER JOIN {{ ref('cee') }} AS c ON j._id = c.carpool_v2_id
  WHERE
    j.valid_acquisition_status = true
    AND j.driver_key IS NOT null
),

cee_first AS (
  SELECT
    driver_key,
    MIN(start_date) AS first_date_cee
  FROM cee_journeys
  GROUP BY driver_key
)

SELECT DISTINCT ON (cj.driver_key)
  cj.driver_key           AS user_id,
  'driver'                AS role,  -- noqa: RF04
  cj.carpool_v2_id::integer,
  cf.first_date_cee::date AS first_date
FROM cee_journeys AS cj
INNER JOIN cee_first AS cf
  ON
    cj.driver_key = cf.driver_key
    AND cj.start_date = cf.first_date_cee
ORDER BY cj.driver_key ASC, cj.carpool_v2_id ASC
