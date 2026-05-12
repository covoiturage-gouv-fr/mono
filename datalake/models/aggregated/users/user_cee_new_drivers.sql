{{
  config(
    materialized='table',
    indexes=[
      { 'columns': ['driver_key'], 'unique': true },
      { 'columns': ['carpool_v2_id'] }
    ],
    tags=['aggregated', 'cee', 'primo_drivers']
  )
}}

WITH cee_journeys AS (
  SELECT
    j._id AS carpool_v2_id,
    j.driver_key,
    j.start_datetime_tz::date AS start_date
  FROM {{ ref('carpools') }} j
  INNER JOIN {{ ref('cee') }} c ON c.carpool_v2_id = j._id
  WHERE j.valid_acquisition_status = true
    AND j.driver_key IS NOT NULL
),

cee_first AS (
  SELECT
    driver_key,
    MIN(start_date) AS first_date_cee
  FROM cee_journeys
  GROUP BY driver_key
)

SELECT DISTINCT ON (cj.driver_key)
  cj.driver_key AS user_id,
  'driver' AS role,
  cj.carpool_v2_id::integer,
  cf.first_date_cee::date AS first_date
FROM cee_journeys cj
INNER JOIN cee_first cf
  ON cf.driver_key = cj.driver_key
  AND cj.start_date = cf.first_date_cee
ORDER BY cj.driver_key, cj.carpool_v2_id ASC