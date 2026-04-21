{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
    indexes = [
      { 'columns':['_id'] },
      { 'columns':['start_datetime'] },
    ],
    tags=['raw', 'journeys']
) }}

WITH filtered_carpools AS (
    SELECT *
    FROM {{ source('carpool_v2', 'carpools') }} c
    WHERE {{ time_filter('c.start_datetime', 'start_datetime', lookback_days=3) }}

),
carpools_status AS (
    SELECT
        s.carpool_id,
        s.updated_at AS status_updated_at,
        s.acquisition_status,
        s.fraud_status,
        s.anomaly_status,
        COALESCE(
            s.acquisition_status IN (
                'processed', 'failed', 'canceled', 'expired', 'terms_violation_error'
            ),
            FALSE
        ) AS final_acquisition_status,
        COALESCE(
            s.acquisition_status = 'processed'
            AND s.anomaly_status = 'passed'
            AND s.fraud_status = 'passed',
            FALSE
        ) AS valid_acquisition_status
    FROM {{ source('carpool_v2', 'status') }} s
    INNER JOIN filtered_carpools c ON s.carpool_id = c._id
),

fraud_labels AS (
    SELECT
        fl.carpool_id,
        ARRAY_AGG(fl.label) AS fraud_labels
    FROM {{ source('fraudcheck', 'labels') }} fl
    INNER JOIN filtered_carpools c  ON fl.carpool_id = c._id
    GROUP BY 1
),

anomaly_labels AS (
    SELECT
        al.carpool_id,
        ARRAY_AGG(al.label) AS anomaly_labels
    FROM {{ source('fraudcheck', 'labels') }} al
    INNER JOIN filtered_carpools c ON al.carpool_id = c._id
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
    ST_X(c.start_position::geometry)::float4 AS start_position_x,
    ST_Y(c.start_position::geometry)::float4 AS start_position_y,
    g.start_geo_code,

    c.end_datetime,
    ST_X(c.end_position::geometry)::float4 AS end_position_x,
    ST_Y(c.end_position::geometry)::float4 AS end_position_y,
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
    md5(COALESCE(
        c.driver_identity_key,
        c.driver_operator_user_id,
        c.driver_phone,
        c.driver_phone_trunc
    )) AS driver_key,

    c.driver_travelpass_name,
    c.driver_travelpass_user_id,
    c.driver_revenue,

    c.passenger_identity_key,
    c.passenger_operator_user_id,
    c.passenger_phone,
    c.passenger_phone_trunc,
    md5(COALESCE(
        c.passenger_identity_key,
        c.passenger_operator_user_id,
        c.passenger_phone,
        c.passenger_phone_trunc
    )) AS passenger_key,

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

FROM filtered_carpools c
LEFT JOIN carpools_status cs ON c._id = cs.carpool_id
LEFT JOIN {{ source('carpool_v2', 'geo') }} g ON g.carpool_id = c._id
LEFT JOIN fraud_labels fl ON c._id = fl.carpool_id
LEFT JOIN anomaly_labels al ON c._id = al.carpool_id
LEFT JOIN {{ source('operator', 'operators') }} o ON c.operator_id = o._id
