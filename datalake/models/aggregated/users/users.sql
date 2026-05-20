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
  ORDER BY user_id, role, incremental_date ASC
)

SELECT
  agg.user_id,
  MIN(incremental_date) FILTER (WHERE agg.role = 'driver')::date  AS first_date_driver,
  MIN(incremental_date) FILTER (WHERE agg.role = 'passenger')::date AS first_date_passenger,
  MAX(incremental_date) FILTER (WHERE agg.role = 'driver')::date  AS last_date_driver,
  MAX(incremental_date) FILTER (WHERE agg.role = 'passenger')::date AS last_date_passenger,
  d_geo.geo_code AS first_geo_code_driver,
  p_geo.geo_code AS first_geo_code_passenger,
  MAX(incremental_date) AS updated_at
FROM {{ ref('user_od_day') }} agg
LEFT JOIN first_geo d_geo ON d_geo.user_id = agg.user_id AND d_geo.role = 'driver'
LEFT JOIN first_geo p_geo ON p_geo.user_id = agg.user_id AND p_geo.role = 'passenger'
GROUP BY agg.user_id, d_geo.geo_code, p_geo.geo_code
