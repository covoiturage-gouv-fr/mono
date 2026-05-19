{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    merge_update_columns=[
      'last_date_driver',
      'last_date_passenger',
      'updated_at'
    ],
    indexes=[
      { 'columns': ['user_id'], 'unique': true }
    ],
    tags=['aggregated', 'users', 'daily']
  )
}}

WITH user_od_day AS (
  SELECT
    *
  FROM {{ ref('user_od_day') }}
  WHERE {{ time_filter('incremental_date', 'updated_at', type='timestamp', default_start="'2020-01-01 00:00:00'") }}

),
first_geo AS (
  SELECT DISTINCT ON (user_id, role)
    user_id,
    role,
    start_geo_code AS geo_code
  FROM user_od_day
  ORDER BY user_id, role, incremental_date ASC
),

agg AS (
  SELECT
    user_id,
    MIN(incremental_date) FILTER (WHERE role = 'driver')::date AS first_date_driver,
    MIN(incremental_date) FILTER (WHERE role = 'passenger')::date AS first_date_passenger,
    MAX(incremental_date) FILTER (WHERE role = 'driver')::date AS last_date_driver,
    MAX(incremental_date) FILTER (WHERE role = 'passenger')::date AS last_date_passenger,
    MAX(incremental_date) AS updated_at
  FROM user_od_day
  GROUP BY user_id
)

SELECT
a.user_id,

{% if is_incremental() %}

-- FIRST : figé → jamais mis à jour après insertion
a.first_date_driver,
a.first_date_passenger,

-- LAST : mis à jour via merge
GREATEST(
  COALESCE(b.last_date_driver, a.last_date_driver),
  a.last_date_driver
) AS last_date_driver,

GREATEST(
  COALESCE(b.last_date_passenger, a.last_date_passenger),
  a.last_date_passenger
) AS last_date_passenger,

-- GEO : uniquement basé sur le batch courant (cohérent avec first figé)
d_geo.geo_code AS first_geo_code_driver,
p_geo.geo_code AS first_geo_code_passenger,

GREATEST(b.updated_at, a.updated_at) AS updated_at

FROM agg a
LEFT JOIN {{ this }} b ON b.user_id = a.user_id
LEFT JOIN first_geo d_geo ON d_geo.user_id = a.user_id AND d_geo.role = 'driver'
LEFT JOIN first_geo p_geo ON p_geo.user_id = a.user_id AND p_geo.role = 'passenger'

{% else %}

a.first_date_driver,
a.first_date_passenger,
a.last_date_driver,
a.last_date_passenger,
d_geo.geo_code AS first_geo_code_driver,
p_geo.geo_code AS first_geo_code_passenger,
a.updated_at

FROM agg a
LEFT JOIN first_geo d_geo ON d_geo.user_id = a.user_id AND d_geo.role = 'driver'
LEFT JOIN first_geo p_geo ON p_geo.user_id = a.user_id AND p_geo.role = 'passenger'

{% endif %}

