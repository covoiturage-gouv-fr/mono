{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'semester_epci_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_epci(model_column='incremental_date',lookback_nb=0, lookback_unit='semester')}}
)

SELECT
  start_epci AS code,
  'epci' AS type,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_epci IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('semester', 3)}}



