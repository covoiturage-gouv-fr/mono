MODEL (
  name raw_zone.incentives,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2019-01-01 00:00:00+0100',
  cron '@daily',
  grain '_id',
  tags ['raw', 'campaign', 'incentives'],
  audits (
    assert_incentives_row_count_pg_to_raw,
    assert_incentives_missing_rows_pg_to_raw,
    assert_incentives_key_fields_pg_to_raw,
  ),
);

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
    AND pi.datetime >= @start_ts
    AND pi.datetime < @end_ts

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
    AND pi.datetime >= @start_ts
    AND pi.datetime < @end_ts
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
JOIN ni ON pi._id = ni._id
LEFT JOIN policy.policies pp ON pi.policy_id = pp._id
LEFT JOIN territory.territory_group ttg ON pp.territory_id = ttg._id
LEFT JOIN company.companies ccp ON ttg.company_id = ccp._id

WHERE pi.datetime >= @start_ts
  AND pi.datetime < @end_ts
;

@create_indexes(
  'incentives_carpool_v2_id_index ON raw_zone.incentives (carpool_v2_id)',
);
