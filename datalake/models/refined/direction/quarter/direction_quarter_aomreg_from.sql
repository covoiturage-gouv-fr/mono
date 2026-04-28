{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'quarter_aomreg_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_aomreg(model_column='incremental_date',lookback_nb=0, lookback_unit='quarter')}}
)

SELECT
  start_aomreg AS code,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_aomreg IS NOT NULL
GROUP BY 1, {{group_by_grain('quarter', 2)}}



