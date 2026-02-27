MODEL (
  name trusted_zone.journeys_2022,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30
  ),
  start '2022-01-01',
  end '2023-01-01',
  grain '_id',
  tags ['trusted', 'journeys', '2022'],
);

SELECT
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
  ST_Point(start_position_x, start_position_y) AS start_position,
  start_h3_index::h3index AS start_h3_index,
  start_geo_code,
  end_datetime,
  end_datetime_tz,
  ST_Point(end_position_x, end_position_y) AS end_position,
  end_h3_index::h3index AS end_h3_index,
  end_geo_code,
  geo_errors,
  geo_updated_at,
  distance,
  duration,
  licence_plate,
  driver_identity_key,
  driver_operator_user_id,
  driver_phone,
  driver_phone_trunc,
  driver_id,
  driver_travelpass_name,
  driver_travelpass_user_id,
  driver_revenue,
  passenger_identity_key,
  passenger_operator_user_id,
  passenger_phone,
  passenger_phone_trunc,
  passenger_id,
  passenger_travelpass_name,
  passenger_travelpass_user_id,
  passenger_over_18,
  passenger_seats,
  passenger_contribution,
  passenger_payments,
  operator_incentives_sirets,
  operator_incentives_amount_total,
  policy_id,
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
FROM raw_zone.journeys_2022
WHERE start_datetime >= @start_ds
AND start_datetime < @end_ds;

CREATE INDEX IF NOT EXISTS journeys_2022_id_index ON @this_model USING btree (_id);
CREATE INDEX IF NOT EXISTS journeys_2022_start_datetime_tz_index ON @this_model USING btree (start_datetime_tz);
CREATE INDEX IF NOT EXISTS journeys_2022_start_h3_index_index ON @this_model USING btree (start_h3_index);
CREATE INDEX IF NOT EXISTS journeys_2022_end_h3_index_index ON @this_model USING btree (end_h3_index);