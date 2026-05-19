{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['user_id', 'incremental_date', 'role', 'start_code', 'end_code'],
    indexes=[
      { 'columns': ['user_id'] },
      { 'columns': ['incremental_date'] },
      { 'columns': ['start_code', 'end_code'] }
    ],
    tags=['aggregated', 'users', 'od', 'daily']
  )
}}

WITH filtered_carpools AS (
  {{ filtered_carpools('arr', lookback_nb=3, lookback_unit='day', with_new_users=false) }}
),

carpools_by_role AS (
  SELECT driver_key AS user_id, 'driver'::text AS role, fc.*
  FROM filtered_carpools fc
  WHERE driver_key IS NOT NULL
  UNION ALL
  SELECT passenger_key AS user_id, 'passenger'::text AS role, fc.*
  FROM filtered_carpools fc
  WHERE passenger_key IS NOT NULL
)

SELECT
  user_id,
  role,
  carpool_datetime::date AS incremental_date,
  start_code,
  end_code,
  {{ user_agg_columns() }}
FROM carpools_by_role
GROUP BY 1, 2, 3, 4, 5
