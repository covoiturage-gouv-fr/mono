MODEL (
  name trusted_zone.journeys,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2020-01-01',
  grain '_id',
);

with carpools as ( 
  select
    c.*,
    -- Add data from other sources
    s.updated_at
    as status_updated_at,
    s.acquisition_status,
    s.fraud_status,
    s.anomaly_status,
    g.start_geo_code,
    g.end_geo_code,
    g.updated_at
    as geo_updated_at,
    g.errors
    as geo_errors,
    -- datetimes are timezoned
    @get_timezoned_timestamp(g.start_geo_code, c.start_datetime) as start_datetime_tz,
    @get_timezoned_timestamp(g.end_geo_code, c.end_datetime) as end_datetime_tz,
    -- Enrich with useful status columns
    coalesce(
      s.acquisition_status in @get_final_acquisition_status_list(),
      FALSE
    ) as final_acquisition_status,
    coalesce(
      s.acquisition_status = 'processed'
      and s.anomaly_status = 'passed'
      and s.fraud_status = 'passed',
      FALSE
    )
    as valid_acquisition_status
  from carpool_v2.carpools as c
  left join carpool_v2.status as s on c._id = s.carpool_id
  left join carpool_v2.geo as g on c._id = g.carpool_id
  where c.start_datetime between @start_date and @end_date
),

fraud_labels as (
  select
    fl.carpool_id,
    array_agg(fl.label) as fraud_labels
  from fraudcheck.labels as fl
  inner join carpools as c on fl.carpool_id = c._id
  group by 1
),

anomaly_labels as (
  select
    al.carpool_id,
    array_agg(al.label) as anomaly_labels
  from fraudcheck.labels as al
  inner join carpools as c on al.carpool_id = c._id
  group by 1
),

operator_incentives as (
  select
    oi.carpool_id,
    array_agg(distinct oi.siret) as operator_incentives_sirets,
    sum(oi.amount)               as operator_incentives_amount_total
  from carpool_v2.operator_incentives as oi
  inner join carpools as c on oi.carpool_id = c._id
  where oi.amount > 0
  group by 1
),

-- Pre-aggregate operator incentives for selected carpools
policy_incentives as (
  select
    pi.carpool_id,
    sum(pi.amount) as policy_incentives_amount_total,
    sum(pi.result) as policy_incentives_result_total
  from policy.incentives as pi
  inner join carpools as c on pi.carpool_id = c._id
  where pi.status = 'validated' -- Filter out non validated amounts
  group by 1
),

joined_data as ( -- Join carpool enriched data with aggregated datasets
  select
    c.*,
    o.name
    as operator_name,
    o.siret
    as operator_siret,
    fl.fraud_labels,
    al.anomaly_labels,
    oi.operator_incentives_sirets,
    oi.operator_incentives_amount_total,
    pi.policy_incentives_amount_total,
    pi.policy_incentives_result_total
  from carpools as c
  left join fraud_labels as fl on c._id = fl.carpool_id
  left join anomaly_labels as al on c._id = al.carpool_id
  left join operator_incentives as oi on c._id = oi.carpool_id
  left join policy_incentives as pi on c._id = pi.carpool_id
  left join
    operator.operators as o
    on c.operator_id = o._id
)

select
  _id,
  created_at,
  updated_at,
  operator_id,
  operator_name,
  operator_siret,
  operator_journey_id,
  operator_trip_id,
  operator_class,
  start_datetime,
  start_datetime_tz,
  start_position,
  start_geo_code,
  end_datetime,
  end_datetime_tz,
  end_position,
  end_geo_code,
  geo_errors,
  geo_updated_at,
  distance,
  licence_plate,
  driver_identity_key,
  driver_operator_user_id,
  driver_phone,
  driver_phone_trunc,
  driver_travelpass_name,
  driver_travelpass_user_id,
  driver_revenue,
  passenger_identity_key,
  passenger_operator_user_id,
  passenger_phone,
  passenger_phone_trunc,
  passenger_travelpass_name,
  passenger_travelpass_user_id,
  passenger_over_18,
  passenger_seats,
  passenger_contribution,
  passenger_payments,
  operator_incentives_sirets,
  operator_incentives_amount_total,
  policy_incentives_amount_total,
  policy_incentives_result_total,
  fraud_status,
  fraud_labels,
  anomaly_status,
  anomaly_labels,
  acquisition_status,
  status_updated_at,
  final_acquisition_status,
  valid_acquisition_status,
  uuid,
  legacy_id
from joined_data;

CREATE INDEX IF NOT EXISTS journeys_id_index ON @this_model USING btree (_id);