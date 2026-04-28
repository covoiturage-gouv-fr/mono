{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code','incremental_date'],
    indexes = [
      { 'columns':['code','incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'day_aom_to']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_aom(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
)

SELECT 
  end_aom AS code, 
  {{incremental_columns('carpool_datetime', 'day')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE end_aom IS NOT NULL
GROUP BY 1, {{group_by_grain('day', 2)}}
