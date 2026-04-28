{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'year_arr_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_arr(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT
  start_arr AS code,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, {{group_by_grain('year', 2)}}



