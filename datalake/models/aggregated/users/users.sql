{{
  config(
    materialized='table',
    indexes=[
      { 'columns': ['user_id'], 'unique': true }
    ],
    tags=['aggregated', 'users', 'daily']
  )
}}

WITH first_geo AS (
  SELECT DISTINCT ON (user_id, role)
    user_id,
    role,
    start_code AS geo_code
  FROM {{ ref('user_od_day') }}
  ORDER BY user_id ASC, role ASC, incremental_date ASC
)

SELECT
  agg.user_id,
  d_geo.geo_code
    AS first_geo_code_driver,
  p_geo.geo_code
    AS first_geo_code_passenger,
  MIN(agg.incremental_date) FILTER (WHERE agg.role = 'driver')::date
    AS first_date_driver,
  MIN(agg.incremental_date) FILTER (WHERE agg.role = 'passenger')::date
    AS first_date_passenger,
  MAX(agg.incremental_date) FILTER (WHERE agg.role = 'driver')::date
    AS last_date_driver,
  MAX(agg.incremental_date) FILTER (WHERE agg.role = 'passenger')::date
    AS last_date_passenger,
  MAX(agg.incremental_date)
    AS updated_at
FROM {{ ref('user_od_day') }} AS agg
LEFT JOIN
  first_geo AS d_geo
  ON agg.user_id = d_geo.user_id AND d_geo.role = 'driver'
LEFT JOIN
  first_geo AS p_geo
  ON agg.user_id = p_geo.user_id AND p_geo.role = 'passenger'
GROUP BY agg.user_id, d_geo.geo_code, p_geo.geo_code
