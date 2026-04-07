MODEL (
  name raw_zone.journeys,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 3,
    batch_size 30
  ),
  start '2019-01-01 00:00:00+0100',
  cron '@daily',
  grain ['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
  tags ['raw', 'journeys'],
  audits (
    assert_journeys_row_count_pg_to_raw,
    assert_journeys_missing_rows_pg_to_raw,
    assert_journeys_key_fields_pg_to_raw,
  ),
);

WITH carpools_status AS (
  SELECT
    s.carpool_id,
    s.updated_at AS status_updated_at,
    s.acquisition_status,
    s.fraud_status,
    s.anomaly_status,
    COALESCE(
      s.acquisition_status IN ('processed', 'failed', 'canceled', 'expired', 'terms_violation_error'),
      FALSE
    ) AS final_acquisition_status,
    COALESCE(
      s.acquisition_status = 'processed'
      AND s.anomaly_status = 'passed'
      AND s.fraud_status = 'passed',
      FALSE
    ) AS valid_acquisition_status
  FROM carpool_v2.status AS s
  INNER JOIN carpool_v2.carpools AS c ON s.carpool_id = c._id
  WHERE c.start_datetime >= @start_ts
    AND c.start_datetime < @end_ts
),

fraud_labels AS (
  SELECT
    fl.carpool_id,
    ARRAY_AGG(fl.label) AS fraud_labels
  FROM fraudcheck.labels AS fl
  INNER JOIN carpool_v2.carpools AS c ON fl.carpool_id = c._id
  WHERE c.start_datetime >= @start_ts
    AND c.start_datetime < @end_ts
  GROUP BY 1
),

anomaly_labels AS (
  SELECT
    al.carpool_id,
    ARRAY_AGG(al.label) AS anomaly_labels
  FROM fraudcheck.labels AS al
  INNER JOIN carpool_v2.carpools AS c ON al.carpool_id = c._id
  WHERE c.start_datetime >= @start_ts
    AND c.start_datetime < @end_ts
  GROUP BY 1
)

SELECT
  c._id,
  c.uuid::VARCHAR AS uuid,
  c.legacy_id,
  c.created_at,
  c.updated_at,

  c.operator_id,
  o.name AS operator_name,
  o.siret AS operator_siret,

  c.operator_journey_id,
  c.operator_trip_id,
  c.operator_class,

  c.start_datetime,
  st_x(c.start_position::geometry)::float4 AS start_position_x,
  st_y(c.start_position::geometry)::float4 AS start_position_y,
  g.start_geo_code,

  c.end_datetime,
  st_x(c.end_position::geometry)::float4 AS end_position_x,
  st_y(c.end_position::geometry)::float4 AS end_position_y,
  h3_lat_lng_to_cell((c.end_position::geometry)::point, 9) AS end_h3_index,
  g.end_geo_code,

  g.errors AS geo_errors,
  g.updated_at AS geo_updated_at,

  c.distance,
  EXTRACT(EPOCH FROM c.end_datetime - c.start_datetime)::integer AS duration,

  c.licence_plate,
  c.driver_identity_key,
  c.driver_operator_user_id,
  c.driver_phone,
  c.driver_phone_trunc,
  coalesce(
    c.driver_identity_key,
    c.driver_operator_user_id,
    c.driver_phone,
    c.driver_phone_trunc
  ) AS driver_id,
  c.driver_travelpass_name,
  c.driver_travelpass_user_id,
  c.driver_revenue,

  c.passenger_identity_key,
  c.passenger_operator_user_id,
  c.passenger_phone,
  c.passenger_phone_trunc,
  coalesce(
    c.passenger_identity_key,
    c.passenger_operator_user_id,
    c.passenger_phone,
    c.passenger_phone_trunc
  ) AS passenger_id,
  c.passenger_travelpass_name,
  c.passenger_travelpass_user_id,
  c.passenger_over_18,
  c.passenger_seats,
  c.passenger_contribution,
  c.passenger_payments,

  cs.fraud_status::VARCHAR AS fraud_status,
  fl.fraud_labels,
  cs.anomaly_status::VARCHAR AS anomaly_status,
  al.anomaly_labels,
  cs.acquisition_status::VARCHAR AS acquisition_status,
  cs.status_updated_at,
  cs.final_acquisition_status,
  cs.valid_acquisition_status

FROM carpool_v2.carpools AS c

LEFT JOIN carpools_status AS cs ON c._id = cs.carpool_id
LEFT JOIN carpool_v2.geo AS g ON g.carpool_id = c._id
LEFT JOIN fraud_labels AS fl ON c._id = fl.carpool_id
LEFT JOIN anomaly_labels AS al ON c._id = al.carpool_id
LEFT JOIN operator.operators AS o ON c.operator_id = o._id

WHERE c.start_datetime >= @start_ts
  AND c.start_datetime < @end_ts
;

@create_indexes(
  'journeys_id_index ON raw_zone.journeys (_id)',
  'journeys_start_datetime_index ON raw_zone.journeys (start_datetime)',
);
