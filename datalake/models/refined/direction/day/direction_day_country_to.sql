{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type','incremental_date'],
    indexes = [
      { 'columns':['code', 'type','incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'day_country_to']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_country(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
)

SELECT 
  end_country AS code, 
  'country' AS type,
  {{incremental_columns('carpool_datetime', 'day')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE end_country IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('day', 3)}}
