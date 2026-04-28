{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code','incremental_date'],
    indexes = [
      { 'columns':['code','incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'day_aom_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_aom(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
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
),
operator_stats AS (
  SELECT
    code,
    operator_id,
    {{incremental_columns('carpool_datetime', 'day')}},
    COUNT(*) AS operator_carpools,
  FROM exploded
  WHERE code IS NOT NULL
  GROUP BY 1, {{group_by_grain('day', 2)}}
),
operator_agg AS (
  SELECT
    code,
    incremental_date,
    jsonb_object_agg(operator_id::text, operator_carpools) AS carpools_by_operator,
  FROM operator_stats
  GROUP BY 1, 2
)

SELECT
  code,
  {{incremental_columns('carpool_datetime', 'day')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, {{group_by_grain('day', 2)}}
