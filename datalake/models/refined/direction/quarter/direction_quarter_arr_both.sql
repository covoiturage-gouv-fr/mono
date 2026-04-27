{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'quarter_arr_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_arr(model_column='incremental_date',lookback_nb=1, lookback_unit='quarter')}}
),

exploded AS (
  SELECT *, start_arr AS code
  FROM filtered_carpools
  WHERE start_arr IS NOT NULL
  UNION ALL
  SELECT *, end_arr AS code
  FROM filtered_carpools
  WHERE end_arr IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  'arr' AS type,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{direction_agg_columns()}}
FROM exploded
GROUP BY 1, 2, {{group_by_grain('quarter', 3)}}
