{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2',  'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'daily', 'semester_reg']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_reg(model_column='incremental_date',lookback_nb=0, lookback_unit='semester')}}
)

SELECT
  least(start_reg, end_reg) AS territory_1,
  greatest(start_reg, end_reg) AS territory_2,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{od_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, {{group_by_grain('semester', 3)}}
