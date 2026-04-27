{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'year_reg_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_reg(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
),

exploded AS (
  SELECT *, start_reg AS code
  FROM filtered_carpools
  WHERE start_reg IS NOT NULL
  UNION ALL
  SELECT *, end_reg AS code
  FROM filtered_carpools
  WHERE end_reg IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  'reg' AS type,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('year', 3)}}