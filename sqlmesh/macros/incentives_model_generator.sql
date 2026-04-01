{#
  incentives_model_generator(start_ts, end_ts)

  Generates the incentives SELECT from live policy.incentives source tables.

  Usage:
    - raw_zone yearly models (raw_zone.incentives_YYYY)
    - raw_zone _latest model (raw_zone.incentives_latest)

  Data sources: policy.incentives, carpool_v2.carpools, carpool.carpools,
  policy.policies, territory.territory_group, company.companies.
  Must NOT reference trusted_zone, refined_zone or any materialised model.
#}
{% macro incentives_model_generator(start_ts, end_ts) %}
WITH ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool_v2.carpools c2
    ON pi.operator_id = c2.operator_id AND pi.operator_journey_id = c2.operator_journey_id
  WHERE pi.operator_id IS NOT NULL 
    AND pi.operator_journey_id IS NOT NULL
    AND pi.datetime >= {{start_ts}}::timestamp - INTERVAL '1 day'
    AND pi.datetime < {{end_ts}}::timestamp + INTERVAL '1 day'

  UNION

  -- fallback to Carpool V1 _id
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool.carpools c1 ON pi.carpool_id = c1._id
  LEFT JOIN carpool_v2.carpools c2 ON c1.acquisition_id = c2.legacy_id
  WHERE pi.carpool_id IS NOT NULL
    AND (pi.operator_id IS NULL OR pi.operator_journey_id IS NULL)
    AND pi.datetime >= {{start_ts}}::timestamp - INTERVAL '1 day'
    AND pi.datetime < {{end_ts}}::timestamp + INTERVAL '1 day'
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

FROM policy.incentives pi
JOIN ni                                  ON pi._id = ni._id
LEFT JOIN policy.policies pp             ON pi.policy_id    = pp._id
LEFT JOIN territory.territory_group ttg  ON pp.territory_id = ttg._id
LEFT JOIN company.companies ccp          ON ttg.company_id  = ccp._id

WHERE pi.datetime >= {{start_ts}}::timestamp - INTERVAL '1 day'
  AND pi.datetime < {{end_ts}}::timestamp + INTERVAL '1 day'
;
{% endmacro %}
