{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'month_dep_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_dep(model_column='incremental_date',lookback_nb=2, lookback_unit='month')}}
),

exploded AS (
  SELECT *, start_dep AS code
  FROM filtered_carpools
  WHERE start_dep IS NOT NULL
  UNION ALL
  SELECT *, end_dep AS code
  FROM filtered_carpools
  WHERE end_dep IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  'dep' AS type,
  {{incremental_columns('carpool_datetime', 'month')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('month', 3)}}