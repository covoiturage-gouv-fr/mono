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
    tags=['trusted', 'users', 'od', 'daily']
  )
}}

WITH daily AS (
  SELECT *
  FROM {{ ref('user_od_day') }}
  WHERE {{ time_filter('incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=1, lookback_unit='month') }}
)


SELECT
  user_id,
  role,
  start_geo_code,
  end_geo_code,
  {{incremental_columns('incremental_date', 'month') }},
  SUM(carpools) AS carpools
FROM daily
GROUP BY 1, 2, 3, 4, {{group_by_grain('month', 5)}}