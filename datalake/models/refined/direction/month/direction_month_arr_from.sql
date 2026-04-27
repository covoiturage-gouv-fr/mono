{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'month_arr_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_arr(model_column='incremental_date',lookback_nb=2, lookback_unit='month')}}
)

SELECT
  start_arr AS code,
  'arr' AS type,
  {{incremental_columns('carpool_datetime', 'month')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, {{group_by_grain('month', 3)}}



