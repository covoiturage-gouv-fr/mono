MODEL (
  name refined_zone.part_campaigns_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01',
  grain ['campaign_id', 'start_date', 'operator_id'],
  tags ['refined', 'partenaires']
);

SELECT 
  b._id as campaign_id,
  a.start_datetime_tz::date as start_date,
  a.operator_id,
  COUNT(distinct a._id) AS journeys,
  COUNT(distinct a._id) FILTER (WHERE a.policy_incentives_amount_total > 0) AS incented_journeys,
  SUM(a.policy_incentives_amount_total) AS incentive_amount
FROM trusted_zone.journeys a
LEFT JOIN policy.policies b ON a.policy_id = b._id
WHERE a.valid_acquisition_status
GROUP BY 1, 2, 3;
