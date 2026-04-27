{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'year_aomreg_to']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_aomreg(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT 
  end_aomreg AS code, 
  'aom' AS type,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE end_aomreg IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('year', 3)}}