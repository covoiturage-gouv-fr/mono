{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'semester_plm']
) }}

WITH filtered_carpools AS (
  {{filtered_carpools_arr(model_column='incremental_date',lookback_nb=0, lookback_unit='semester')}}
)

SELECT
  least(start_com, end_com) AS territory_1,
  greatest(start_com, end_com) AS territory_2,
  'com' AS type,
  {{incremental_columns('carpool_datetime', 'semester')}},
  {{od_agg_columns()}}
FROM filtered_carpools
WHERE start_com IN ('75056', '69123', '13055')
  OR end_com IN ('75056', '69123', '13055')
GROUP BY 1, 2, 3, {{group_by_grain('semester', 4)}}
