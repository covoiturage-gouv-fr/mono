{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2',  'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'daily', 'year_arr']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_arr(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT
  least(start_arr, end_arr) AS territory_1,
  greatest(start_arr, end_arr) AS territory_2,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{od_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, {{group_by_grain('year', 3)}}
