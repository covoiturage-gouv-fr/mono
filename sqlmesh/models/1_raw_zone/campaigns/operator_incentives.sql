MODEL (
  name raw_zone.operator_incentives,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2019-01-01 00:00:00+0100',
  cron '@daily',
  grain ['carpool_id', 'siret'],
  tags ['raw', 'campaign', 'operator_incentives'],
);

SELECT
  t.carpool_id,
  c.start_datetime,
  t.siret,
  comp.legal_name,
  t.amount
FROM carpool_v2.operator_incentives t
INNER JOIN carpool_v2.carpools c ON c._id = t.carpool_id
LEFT JOIN company.companies comp ON comp.siret = t.siret
WHERE t.amount > 0
  AND c.start_datetime >= @start_ts
  AND c.start_datetime < @end_ts
;

@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS operator_incentives_carpool_id_index ON raw_zone.operator_incentives (carpool_id));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS operator_incentives_start_datetime_index ON raw_zone.operator_incentives (start_datetime));
