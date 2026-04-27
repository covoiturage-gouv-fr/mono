{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type','incremental_date'],
    indexes = [
      { 'columns':['code', 'type','incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'day_plm_to']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_plm(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
)

SELECT
  end_com AS code,
  'com' AS type,
  {{incremental_columns('carpool_datetime', 'day')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE end_com IN ('75056', '69123', '13055')
GROUP BY 1, 2, {{group_by_grain('day', 3)}}



