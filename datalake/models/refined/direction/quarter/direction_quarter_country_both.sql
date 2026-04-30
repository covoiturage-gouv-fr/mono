{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'daily', 'quarter_country_both']
) }}
WITH filtered_carpools AS (
  {{filtered_carpools_country(model_column='incremental_date',lookback_nb=1, lookback_unit='quarter')}}
),

exploded AS (
  SELECT *, start_country AS code
  FROM filtered_carpools
  WHERE start_country IS NOT NULL
  UNION ALL
  SELECT *, end_country AS code
  FROM filtered_carpools
  WHERE end_country IS NOT NULL
    AND NOT is_intra
)

SELECT
  code,
  {{incremental_columns('carpool_datetime', 'quarter')}},
  {{direction_agg_columns()}}
FROM exploded
WHERE code IS NOT NULL
GROUP BY 1, {{group_by_grain('quarter', 2)}}