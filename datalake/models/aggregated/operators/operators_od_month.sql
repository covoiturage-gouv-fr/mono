{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['operator_id', 'incremental_date', 'role', 'start_geo_code', 'end_geo_code'],
    indexes=[
      { 'columns': ['operator_id'] },
      { 'columns': ['incremental_date'] },
      { 'columns': ['start_geo_code', 'end_geo_code'] }
    ],
    tags=['aggregated', 'operators', 'od', 'daily']
  )
}}

WITH daily AS (
  SELECT *
  FROM {{ ref('operators_od_day') }}
  WHERE {{ time_filter('incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=1, lookback_unit='month') }}
)


SELECT
  operator_id,
  start_geo_code,
  end_geo_code,
  {{incremental_columns('incremental_date', 'month') }},
  SUM(carpools) AS carpools
FROM daily
GROUP BY 1, 2, 3, {{group_by_grain('month', 4)}}