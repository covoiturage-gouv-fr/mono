{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code','incremental_date'],
    indexes = [
      { 'columns':['code','incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'day_aomreg_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_aomreg(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
),

exploded AS (
  SELECT *, start_aomreg AS code
  FROM filtered_carpools
  WHERE start_aomreg IS NOT NULL
  UNION ALL
  SELECT *, end_aomreg AS code
  FROM filtered_carpools
  WHERE end_aomreg IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  {{incremental_columns('carpool_datetime', 'day')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, {{group_by_grain('day', 2)}}
