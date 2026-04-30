{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'semester_dep_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_dep(model_column='incremental_date',lookback_nb=1, lookback_unit='semester')}}
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
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, {{group_by_grain('semester', 2)}}