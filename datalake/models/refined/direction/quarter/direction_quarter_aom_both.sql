{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'quarter_aom_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_aom(model_column='incremental_date',lookback_nb=1, lookback_unit='quarter')}}
),

exploded AS (
  SELECT *, start_aom AS code
  FROM filtered_carpools
  WHERE start_aom IS NOT NULL
  UNION ALL
  SELECT *, end_aom AS code
  FROM filtered_carpools
  WHERE end_aom IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  'aom' AS type,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('quarter', 3)}}