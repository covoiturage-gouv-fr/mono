{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'year_reg']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_reg(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT
  least(start_reg, end_reg) AS territory_1,
  greatest(start_reg, end_reg) AS territory_2,
  'reg' AS type,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{od_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, 3, {{group_by_grain('year', 4)}}
