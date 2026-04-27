{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'semester_aom_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_aom(model_column='incremental_date',lookback_nb=0, lookback_unit='semester')}}
)

SELECT
  start_aom AS code,
  'aom' AS type,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_aom IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('semester', 3)}}



