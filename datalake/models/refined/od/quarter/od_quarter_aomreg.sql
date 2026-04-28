{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2',  'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'daily', 'quarter_aomreg']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_aomreg(model_column='incremental_date',lookback_nb=0, lookback_unit='quarter')}}
)

SELECT
  least(start_aomreg, end_aomreg) AS territory_1,
  greatest(start_aomreg, end_aomreg) AS territory_2,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{od_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, {{group_by_grain('quarter', 3)}}
