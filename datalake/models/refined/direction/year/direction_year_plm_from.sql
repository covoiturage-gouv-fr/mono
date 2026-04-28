{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'year_plm_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_plm(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT
  start_com AS code,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_com IN ('75056', '69123', '13055')
GROUP BY 1, {{group_by_grain('year', 2)}}



