{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id'],
    indexes = [
      { 'columns':['carpool_v2_id'] },
    ],
    tags=['raw', 'incentives'],
) }}

WITH incentives AS (
  SELECT *
  FROM {{ source('policy', 'incentives') }}
  WHERE {{ time_filter('datetime', lookback_days=3) }}
),

ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM incentives pi
  LEFT JOIN {{ source('carpool_v2', 'carpools') }} c2
    ON pi.operator_id = c2.operator_id AND pi.operator_journey_id = c2.operator_journey_id
  WHERE pi.operator_id IS NOT NULL
    AND pi.operator_journey_id IS NOT NULL

  UNION

  -- fallback to Carpool V1 _id
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM incentives pi
  LEFT JOIN {{ source('carpool_v1', 'carpools') }} c1 ON pi.carpool_id = c1._id
  LEFT JOIN {{ source('carpool_v2', 'carpools') }} c2 ON c1.acquisition_id = c2.legacy_id
  WHERE pi.carpool_id IS NOT NULL
    AND (pi.operator_id IS NULL OR pi.operator_journey_id IS NULL)
)

SELECT DISTINCT
  pi._id,
  ni.carpool_v2_id,
  pi.datetime,
  ni.operator_id,
  ni.operator_journey_id,
  pi.policy_id AS campaign_id,
  pp.name AS campaign_name,
  ccp.siret AS territory_siret,
  ttg.name AS territory_name,
  pi.amount,
  pi.result,
  pi.status::VARCHAR AS status,
  pi.state::VARCHAR AS state

FROM incentives pi
JOIN ni ON pi._id = ni._id
LEFT JOIN {{ source('policy', 'policies') }} pp ON pi.policy_id = pp._id
LEFT JOIN {{ source('territory', 'territory_group') }} ttg ON pp.territory_id = ttg._id
LEFT JOIN {{ source('company', 'companies') }} ccp ON ttg.company_id = ccp._id
