{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'year_epci_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_epci(model_column='incremental_date',lookback_nb=0, lookback_unit='year')}}
),

exploded AS (
  SELECT *, start_epci AS code
  FROM filtered_carpools
  WHERE start_epci IS NOT NULL
  UNION ALL
  SELECT *, end_epci AS code
  FROM filtered_carpools
  WHERE end_epci IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  {{incremental_columns('carpool_datetime', 'year')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, {{group_by_grain('year', 2)}}