{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'day_country']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_country(model_column='incremental_date',lookback_nb=3, lookback_unit='day')}}
)

SELECT
  least(start_country, end_country) AS territory_1,
  greatest(start_country, end_country) AS territory_2,
  'country' AS type,
  {{incremental_columns('carpool_datetime', 'day')}},
  {{od_agg_columns()}}
FROM filtered_carpools
GROUP BY 1, 2, 3, {{group_by_grain('day', 4)}}
