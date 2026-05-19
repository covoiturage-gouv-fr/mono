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

WITH carpools AS (
  SELECT
    operator_id,
    start_datetime_tz::date AS carpool_date,
    start_geo_code,
    end_geo_code
  FROM {{ ref('carpools') }}
  WHERE {{ time_filter('start_datetime_tz', 'incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=3) }}
    AND operator_id IS NOT NULL
    AND valid_acquisition_status = true
)

SELECT
  operator_id,
  carpool_date AS incremental_date,
  start_geo_code,
  end_geo_code,
  COUNT(*) AS carpools
FROM carpools
GROUP BY 1, 2, 3, 4