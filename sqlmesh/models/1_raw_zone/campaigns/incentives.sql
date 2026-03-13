-- Version normalisée de la table policy.incentives
-- avec la création d'une FOREIGN KEY unique pour assurer la jointure avec les tables 'journeys'.
--
-- 2 Types de jointures cohabitent dans le temps :
--  - carpool.carpools._id = policy.incentives.carpool_id
--    entre: 2019-05-01 04:40:30+0200 et 2024-09-30 23:56:21+0200
--
--  - carpool_v2.carpools.operator_id = policy.incentives.operator_id
--    AND carpool_v2.carpools.operator_journey_id = policy.incentives.operator_journey_id
--    à partir de: 2023-01-01 01:00:00+0100
--
-- Lors de la migration de carpool -> carpool_v2, les _id de la table ont changé.
--   carpool_v1._id a été conservé dans carpool_v2.legacy_id
-- Pour les incentives calculées avec un carpool_v1, on a :
--   LEFT JOIN carpool_v2.legacy_id = carpool_v1.acquisition_id
--   LEFT JOIN carpool_v1._id = incentives.carpool_id
--
-- carpool_v2_id :
--   carpool_v2.carpools._id = campaign.incentives.carpool_v2_id
--

MODEL (
  name raw_zone.campaign_incentives,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    lookback 7,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  grain '_id',
  tags ['raw', 'campaign', 'incentives'],
);

WITH ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool.carpools c1 ON pi.carpool_id = c1._id
  LEFT JOIN carpool_v2.carpools c2 ON c1.acquisition_id = c2.legacy_id
  WHERE pi.carpool_id IS NOT NULL
    AND pi.datetime BETWEEN @start_ts AND @end_ts

  UNION

  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool_v2.carpools c2
    ON pi.operator_id = c2.operator_id AND pi.operator_journey_id = c2.operator_journey_id
  WHERE pi.operator_id IS NOT NULL AND pi.operator_journey_id IS NOT NULL
    AND pi.datetime BETWEEN @start_ts AND @end_ts
)

SELECT
  pi._id,
  ni.carpool_v2_id,
  pi.datetime,
  ni.operator_id,
  ni.operator_journey_id,
  pi.policy_id as campaign_id,
  pp.name as campaign_name,
  ccp.siret as territory_siret,
  ttg.name as territory_name,
  pi.amount,
  pi.result,
  pi.status,
  pi.state

FROM policy.incentives pi
JOIN ni                                  ON pi._id = ni._id
LEFT JOIN policy.policies pp             ON pi.policy_id    = pp._id
LEFT JOIN territory.territory_group ttg  ON pp.territory_id = ttg._id
LEFT JOIN company.companies ccp          ON ttg.company_id  = ccp._id

