{#
  journeys_model_generator(start_ts, end_ts)

  Generates the base journey SELECT from live carpool_v2 source tables.

  Usage:
    - archive_zone journey models (archive_zone.journeys_YYYY)
    - raw_zone _latest models that ingest live data (raw_zone.journeys_latest)

  Data sources: carpool_v2 schema only (carpools, status, geo) plus
  operator.operators and fraudcheck.labels for enrichment.
  Must NOT reference trusted_zone, refined_zone or any materialised model.
#}
{% macro journeys_model_generator(start_ts, end_ts) %}
    WITH carpools_status AS (
      SELECT
        s.carpool_id,
        s.updated_at AS status_updated_at,
        s.acquisition_status,
        s.fraud_status,
        s.anomaly_status,
        COALESCE(
          s.acquisition_status IN {{get_final_acquisition_status_list()}},
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
    ),

    fraud_labels AS (
      SELECT
        fl.carpool_id,
        ARRAY_AGG(fl.label) AS fraud_labels
      FROM fraudcheck.labels AS fl
      INNER JOIN carpool_v2.carpools AS c ON fl.carpool_id = c._id
      GROUP BY 1
    ),

    anomaly_labels AS (
      SELECT
        al.carpool_id,
        ARRAY_AGG(al.label) AS anomaly_labels
      FROM fraudcheck.labels AS al
      INNER JOIN carpool_v2.carpools AS c ON al.carpool_id = c._id
      GROUP BY 1
    )

    SELECT
      -- carpool_v2.carpools._id
      c._id,
      c.uuid::VARCHAR AS uuid,
      c.legacy_id, -- carpool_v1.carpools.acquisition_id
      c.created_at,
      c.updated_at,

      -- operator
      c.operator_id,
      o.name AS operator_name,
      o.siret AS operator_siret,

      -- journey
      c.operator_journey_id,
      c.operator_trip_id,
      c.operator_class,

      -- start
      c.start_datetime,
      {{get_timezoned_timestamp("g.start_geo_code", "c.start_datetime")}} AS start_datetime_tz,
      st_x(c.start_position::geometry)::float4                           AS start_position_x,
      st_y(c.start_position::geometry)::float4                           AS start_position_y,
      h3_lat_lng_to_cell((c.start_position::geometry)::point, 9)         AS start_h3_index,
      g.start_geo_code,

      -- end
      c.end_datetime,
      {{get_timezoned_timestamp("g.end_geo_code", "c.end_datetime")}} AS end_datetime_tz,
      st_x(c.end_position::geometry)::float4                          AS end_position_x,
      st_y(c.end_position::geometry)::float4                          AS end_position_y,
      h3_lat_lng_to_cell((c.end_position::geometry)::point, 9)        AS end_h3_index,
      g.end_geo_code,

      -- geo
      g.errors    AS geo_errors,
      g.updated_at AS geo_updated_at,

      -- distance and duration
      c.distance,
      EXTRACT(EPOCH FROM c.end_datetime - c.start_datetime)::integer AS duration,

      -- driver
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

      -- passenger
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

      -- fraud status
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
    LEFT JOIN carpool_v2.geo AS g   ON g.carpool_id = c._id
    LEFT JOIN fraud_labels AS fl    ON c._id = fl.carpool_id
    LEFT JOIN anomaly_labels AS al  ON c._id = al.carpool_id
    LEFT JOIN operator.operators AS o ON c.operator_id = o._id

    WHERE c.start_datetime >= {{start_ts}}::timestamp
      AND c.start_datetime <  {{end_ts}}::timestamp
    ;

{% endmacro %}
