{{ config(
  materialized='view',
  tags=['trusted', 'incentives'],
) }}

SELECT
  _id,
  carpool_v2_id,
  datetime,
  operator_id,
  operator_journey_id,
  campaign_id,
  campaign_name,
  territory_siret,
  territory_name,
  amount,
  result,
  status,
  state
FROM {{ ref('raw_incentives') }}