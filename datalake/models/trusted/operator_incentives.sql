{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['carpool_id', 'siret'],
    indexes = [
      { 'columns':['carpool_id'] },
      { 'columns':['start_datetime'] },
    ],
    tags=['trusted', 'incentives', 'operator', 'daily'],
) }}

SELECT
  t.carpool_id,
  c.start_datetime,
  t.siret,
  t.amount
FROM {{ source('carpool_v2', 'operator_incentives') }} AS t
INNER JOIN {{ source('carpool_v2', 'carpools') }} AS c ON t.carpool_id = c._id
WHERE
  {{ time_filter('c.start_datetime', 'start_datetime', lookback_nb=3) }}
  AND t.amount > 0
