{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['carpool_id', 'siret'],
    indexes = [
      { 'columns':['carpool_id'] },
      { 'columns':['start_datetime'] },
    ],
    tags=['raw', 'campaigns', 'operator_incentives'],
) }}

SELECT
  t.carpool_id,
  c.start_datetime,
  t.siret,
  comp.legal_name,
  t.amount
FROM {{ source('carpool_v2', 'operator_incentives') }} t
INNER JOIN {{ source('carpool_v2', 'carpools') }} c ON c._id = t.carpool_id
LEFT JOIN {{ source('company', 'companies') }} comp ON comp.siret = t.siret
 WHERE {{ time_filter('c.start_datetime', 'start_datetime') }}
  AND t.amount > 0