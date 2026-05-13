{{
  config(
    materialized='table',
    indexes=[
      { 'columns': ['operator_id'], 'unique': true }
    ],
    tags=['aggregated', 'operators', 'daily']
  )
}}

SELECT
  operator_id,
  MIN(incremental_date)::date AS first_date,
  MAX(incremental_date)::date AS last_date,
  SUM(carpools) AS carpools
FROM {{ ref('operators_od_day') }}
GROUP BY operator_id
