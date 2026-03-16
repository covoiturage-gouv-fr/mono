{% macro campaign_incentives(start_ts, end_ts) %}

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
    AND pi.datetime BETWEEN {{start_ts}} AND {{end_ts}}

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
    AND pi.datetime BETWEEN {{start_ts}} AND {{end_ts}}
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

ORDER BY pi.datetime;

{% endmacro %}

