{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'year_epci_from']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_epci(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
)

SELECT
  start_epci AS code,
  'epci' AS type,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM filtered_carpools
WHERE start_epci IS NOT NULL
GROUP BY 1, 2, {{group_by_grain('year', 3)}}



