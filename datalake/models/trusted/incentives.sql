{#
  Index sur _id = clé du delete+insert. Sans lui, le DELETE incrémental balaie toute la
  table (~32,6 M lignes, ~9,6 Go) à chaque exécution => Seq Scan complet. carpool_v2_id
  reste indexé pour les jointures aval.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id'],
    indexes = [
      { 'columns':['_id'] },
      { 'columns':['carpool_v2_id'] },
    ],
    tags=['trusted', 'incentives', 'daily'],
) }}

WITH incentives AS (
  SELECT *
  FROM {{ source('dlk_import', 'policy_incentives') }}
  WHERE {{ time_filter('datetime', lookback_nb=3) }}
),

ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM incentives AS pi
  LEFT JOIN {{ source('dlk_import', 'carpool_v2_carpools') }} AS c2
    ON
      pi.operator_id = c2.operator_id
      AND pi.operator_journey_id = c2.operator_journey_id
  WHERE
    pi.operator_id IS NOT NULL
    AND pi.operator_journey_id IS NOT NULL

  UNION

  -- fallback to Carpool V1 _id
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM incentives AS pi
  LEFT JOIN
    {{ source('dlk_import', 'carpool_carpools') }} AS c1
    ON pi.carpool_id = c1._id
  LEFT JOIN
    {{ source('dlk_import', 'carpool_v2_carpools') }} AS c2
    ON c1.acquisition_id = c2.legacy_id
  WHERE
    pi.carpool_id IS NOT NULL
    AND (pi.operator_id IS NULL OR pi.operator_journey_id IS NULL)
)

SELECT DISTINCT
  pi._id,
  ni.carpool_v2_id,
  pi.datetime,
  ni.operator_id,
  ni.operator_journey_id,
  pi.policy_id       AS campaign_id,
  pp.name            AS campaign_name,
  ccp.siret          AS territory_siret,
  ttg.name           AS territory_name,
  pi.amount,
  pi.result,
  pi.status::VARCHAR AS status,
  pi.state::VARCHAR  AS state

FROM incentives AS pi
INNER JOIN ni ON pi._id = ni._id
LEFT JOIN
  {{ source('dlk_import', 'policy_policies') }} AS pp
  ON pi.policy_id = pp._id
LEFT JOIN
  {{ source('dlk_import', 'territory_territory_group') }} AS ttg
  ON pp.territory_id = ttg._id
LEFT JOIN
  {{ source('dlk_import', 'company_companies') }} AS ccp
  ON ttg.company_id = ccp._id
