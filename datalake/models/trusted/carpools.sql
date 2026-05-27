{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
    indexes = [
      { 'columns':['_id'] },
      { 'columns':['start_datetime_tz'] },
      { 'columns':['start_h3index_z9'] },
      { 'columns':['end_h3index_z9'] },
      { 'columns':['start_h3index_z8'] },
      { 'columns':['end_h3index_z8'] },
      { 'columns': ['driver_key'] },
      { 'columns': ['passenger_key'] },
      { 'columns': ['valid_acquisition_status'] }
    ],
    tags=['trusted', 'carpools', 'daily']
) }}

WITH source_carpools AS (
    SELECT *
    FROM {{ source('carpool_v2', 'carpools') }} c
    WHERE {{ time_filter('c.start_datetime', 'start_datetime', lookback_nb=3) }}
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
    INNER JOIN source_carpools c ON s.carpool_id = c._id
),

fraud_labels AS (
    SELECT
        fl.carpool_id,
        ARRAY_AGG(fl.label) AS fraud_labels
    FROM {{ source('fraudcheck', 'labels') }} fl
    INNER JOIN source_carpools c ON fl.carpool_id = c._id
    GROUP BY 1
),

anomaly_labels AS (
    SELECT
        al.carpool_id,
        ARRAY_AGG(al.label) AS anomaly_labels
    FROM {{ source('anomaly', 'labels') }} al
    INNER JOIN source_carpools c ON al.carpool_id = c._id
    GROUP BY 1
),

terms_violation_error_labels AS (
    SELECT tv.carpool_id, tv.labels AS terms_violation_error_labels
    FROM {{ source('carpool_v2', 'terms_violation_error_labels') }} tv
    INNER JOIN source_carpools c ON tv.carpool_id = c._id
),

operators AS (
    SELECT DISTINCT left(siret, 9) AS code, name AS libelle
    FROM {{ source('operator', 'operators') }}
),

collectivites AS (
    SELECT DISTINCT code, libelle
    FROM {{ ref('perimeters_agg') }}
    WHERE type IN ('epci', 'aom')
),

operator_incentives_agg AS (
    SELECT
        oi.carpool_id,
        SUM(oi.amount) FILTER (WHERE c.code IS NOT NULL) AS oi_amount_collectivite,
        SUM(oi.amount) FILTER (WHERE op.code IS NOT NULL) AS oi_amount_operator,
        SUM(oi.amount) FILTER (WHERE c.code IS NULL AND op.code IS NULL AND oi.siret IS NOT NULL) AS oi_amount_other,
        COUNT(*) FILTER (WHERE c.code IS NOT NULL) AS oi_collectivite,
        COUNT(*) FILTER (WHERE op.code IS NOT NULL) AS oi_operator,
        COUNT(*) FILTER (WHERE c.code IS NULL AND op.code IS NULL AND oi.siret IS NOT NULL) AS oi_other,
        SUM(oi.amount) AS oi_amount_total,
        jsonb_agg(
            jsonb_build_object(
                'siret', oi.siret,
                'type', CASE WHEN c.code IS NOT NULL THEN 'collectivite' WHEN op.code IS NOT NULL THEN 'operator' ELSE 'other' END,
                'name', CASE WHEN c.code IS NOT NULL THEN c.libelle WHEN op.code IS NOT NULL THEN op.libelle ELSE NULL END,
                'amount', oi.amount
            )
        ) AS oi_details
    FROM {{ ref('operator_incentives') }} oi
    LEFT JOIN operators op ON op.code = left(oi.siret, 9)
    LEFT JOIN collectivites c ON c.code = left(oi.siret, 9)
    WHERE {{ time_filter('oi.start_datetime', 'start_datetime', lookback_nb=3) }}
        AND oi.amount > 0
    GROUP BY 1
),

campaigns_agg AS (
    SELECT
        carpool_v2_id,
        jsonb_agg(
            jsonb_build_object(
                'campaign_id', campaign_id,
                'campaign_name', campaign_name,
                'siret', territory_siret,
                'name', territory_name,
                'amount', amount,
                'result', result
            )
        ) AS campaigns,
        SUM(amount) AS campaigns_amount_total,
        SUM(result) AS campaigns_result_total
    FROM {{ ref('incentives') }}
    WHERE {{ time_filter('datetime', model_column='start_datetime', lookback_nb=3) }}
    GROUP BY 1
),

base_carpools AS (
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
        c.operator_class::VARCHAR AS operator_class,

        c.start_datetime,
        {{ timezoned_ts('COALESCE(gc.start_geo_code, g.start_geo_code)', 'c.start_datetime') }} AS start_datetime_tz,
        h3_lat_lng_to_cell(c.start_position::geometry::point, 9) AS start_h3index_z9,
        h3_lat_lng_to_cell(c.start_position::geometry::point, 8) AS start_h3index_z8,
        COALESCE(gc.start_geo_code, g.start_geo_code) AS start_geo_code,

        c.end_datetime,
        {{ timezoned_ts('COALESCE(gc.end_geo_code, g.end_geo_code)', 'c.end_datetime') }} AS end_datetime_tz,
        h3_lat_lng_to_cell(c.end_position::geometry::point, 9) AS end_h3index_z9,
        h3_lat_lng_to_cell(c.end_position::geometry::point, 8) AS end_h3index_z8,
        COALESCE(gc.end_geo_code, g.end_geo_code) AS end_geo_code,

        to_jsonb(g.errors) AS geo_errors,
        g.updated_at AS geo_updated_at,

        c.distance,
        EXTRACT(EPOCH FROM c.end_datetime - c.start_datetime)::integer AS duration,

        md5(COALESCE(
            c.driver_identity_key,
            c.driver_operator_user_id,
            c.driver_phone,
            c.driver_phone_trunc
        )) AS driver_key,
        c.driver_revenue,

        md5(COALESCE(
            c.passenger_identity_key,
            c.passenger_operator_user_id,
            c.passenger_phone,
            c.passenger_phone_trunc
        )) AS passenger_key,
        c.passenger_over_18,
        c.passenger_seats,
        c.passenger_contribution,
        c.passenger_payments::jsonb AS passenger_payments,

        oi.oi_details,  
        COALESCE(oi.oi_amount_collectivite, 0) AS oi_amount_collectivite,
        COALESCE(oi.oi_amount_operator, 0) AS oi_amount_operator,
        COALESCE(oi.oi_amount_other, 0) AS oi_amount_other,
        COALESCE(oi.oi_amount_total, 0) AS oi_amount_total,
        COALESCE(oi.oi_collectivite, 0) AS oi_collectivite,
        COALESCE(oi.oi_operator, 0) AS oi_operator,
        COALESCE(oi.oi_other, 0) AS oi_other,
        (COALESCE(oi.oi_amount_total, 0) > 0) AS with_incentive,

        rpc.campaigns,
        rpc.campaigns_amount_total,
        rpc.campaigns_result_total,

        cs.fraud_status::VARCHAR AS fraud_status,
        fl.fraud_labels,
        cs.anomaly_status::VARCHAR AS anomaly_status,
        al.anomaly_labels,
        cs.acquisition_status::VARCHAR AS acquisition_status,
        cs.status_updated_at,
        cs.final_acquisition_status,
        cs.valid_acquisition_status,
        tv.terms_violation_error_labels

    FROM source_carpools c
    LEFT JOIN carpools_status cs ON c._id = cs.carpool_id
    LEFT JOIN {{ source('carpool_v2', 'geo') }} g ON g.carpool_id = c._id
    LEFT JOIN {{ ref('carpools_geo_correction') }} gc ON gc.carpool_id = c._id
    LEFT JOIN fraud_labels fl ON c._id = fl.carpool_id
    LEFT JOIN anomaly_labels al ON c._id = al.carpool_id
    LEFT JOIN terms_violation_error_labels tv ON c._id = tv.carpool_id
    LEFT JOIN {{ source('operator', 'operators') }} o ON c.operator_id = o._id
    LEFT JOIN operator_incentives_agg oi ON oi.carpool_id = c._id
    LEFT JOIN campaigns_agg rpc ON rpc.carpool_v2_id = c._id
)

SELECT
    *,
    EXTRACT('hour' FROM start_datetime_tz)::int AS hour,
    CASE
        WHEN distance <  5000 THEN '00-05'
        WHEN distance < 10000 THEN '05-10'
        WHEN distance < 15000 THEN '10-15'
        WHEN distance < 20000 THEN '15-20'
        WHEN distance < 25000 THEN '20-25'
        WHEN distance < 30000 THEN '25-30'
        WHEN distance < 35000 THEN '30-35'
        WHEN distance < 40000 THEN '35-40'
        WHEN distance < 45000 THEN '40-45'
        WHEN distance < 50000 THEN '45-50'
        WHEN distance < 55000 THEN '50-55'
        WHEN distance < 60000 THEN '55-60'
        WHEN distance < 65000 THEN '60-65'
        WHEN distance < 70000 THEN '65-70'
        WHEN distance < 75000 THEN '70-75'
        WHEN distance < 80000 THEN '75-80'
        ELSE '80+'
    END AS dist_class,
    (start_geo_code = end_geo_code) AS is_intra
FROM base_carpools
