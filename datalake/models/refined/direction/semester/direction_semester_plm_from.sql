{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'semester_plm_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_plm(model_column='incremental_date',lookback_nb=0, lookback_unit='semester')}}
)

SELECT
  start_com AS code,
  'com' AS type,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_com IN ('75056', '69123', '13055')
GROUP BY 1, 2, {{group_by_grain('semester', 3)}}



