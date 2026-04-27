{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'quarter_plm_both']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_plm(model_column='incremental_date',lookback_nb=1, lookback_unit='quarter')}}
),

exploded AS (
  SELECT *, start_com AS code
  FROM filtered_carpools
  WHERE start_com IS NOT NULL
  UNION ALL
  SELECT *, end_com AS code
  FROM filtered_carpools
  WHERE end_com IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  'com' AS type,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IN ('75056', '69123', '13055')
GROUP BY 1, 2, {{group_by_grain('quarter', 3)}}