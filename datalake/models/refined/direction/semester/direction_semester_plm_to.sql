{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'semester_plm_to']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_plm(model_column='incremental_date',lookback_nb=1, lookback_unit='semester')}}
)

SELECT
  end_com AS code,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE end_com IN ('75056', '69123', '13055')
GROUP BY 1, {{group_by_grain('semester', 2)}}



