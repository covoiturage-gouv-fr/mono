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
        c.operator_class,

        c.start_datetime,
        c.start_position::geometry AS start_position,
        g.start_geo_code,

        c.end_datetime,
        c.end_position::geometry AS end_position,
        g.end_geo_code,

        g.errors AS geo_errors,
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
        c.passenger_payments,

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
    LEFT JOIN fraud_labels fl ON c._id = fl.carpool_id
    LEFT JOIN anomaly_labels al ON c._id = al.carpool_id
    LEFT JOIN terms_violation_error_labels tv ON c._id = tv.carpool_id
    LEFT JOIN {{ source('operator', 'operators') }} o ON c.operator_id = o._id
),

perimeters_retablissement AS (
  SELECT year, com, geom
  FROM {{ ref('perimeters') }} p
  WHERE com IN (
    SELECT DISTINCT new_com
    FROM {{ ref('com_evolution') }}
    WHERE mod = 21
      AND year = p.year
  )
),

geocoding AS (
  -- Cas 1: Communes non fusionnees (mod = 32)
  SELECT
    g._id as carpool_id,
    COALESCE(cs.new_com, g.start_geo_code) AS start_geo_code,
    COALESCE(ce.new_com, g.end_geo_code) AS end_geo_code,
    g.geo_updated_at,
    g.geo_errors
  FROM base_carpools AS g
  LEFT JOIN {{ ref('com_evolution') }} cs
    ON g.start_geo_code = cs.old_com
    AND cs.mod = 32
    AND cs.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
  LEFT JOIN {{ ref('com_evolution') }} ce
    ON g.end_geo_code = ce.old_com
    AND ce.mod = 32
    AND ce.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
  WHERE NOT EXISTS (
      SELECT 1 FROM {{ ref('com_evolution') }} ce_s
      WHERE ce_s.old_com = g.start_geo_code
        AND ce_s.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
        AND ce_s.mod = 21
    )
    OR NOT EXISTS (
      SELECT 1 FROM {{ ref('com_evolution') }} ce_e
      WHERE ce_e.old_com = g.end_geo_code
        AND ce_e.year = EXTRACT(YEAR FROM g.updated_at)::int
        AND ce_e.mod = 21
    )

  UNION ALL

  -- Cas 2: Communes fusionnees (mod = 21) - utiliser perimetres
  SELECT
    g._id as carpool_id,
    COALESCE(ps.com, g.start_geo_code) AS start_geo_code,
    COALESCE(pe.com, g.end_geo_code) AS end_geo_code,
    g.geo_updated_at,
    g.geo_errors
  FROM base_carpools AS g
  LEFT JOIN perimeters_retablissement ps
    ON ST_Intersects(g.start_position, ps.geom)
    AND ps.year = EXTRACT(YEAR FROM g.updated_at)::int
  LEFT JOIN perimeters_retablissement pe
    ON ST_Intersects(g.end_position, pe.geom)
    AND pe.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
  WHERE EXISTS (
      SELECT 1 FROM {{ ref('com_evolution') }} ce_s
      WHERE ce_s.old_com = g.start_geo_code
        AND ce_s.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
        AND ce_s.mod = 21
    )
    OR EXISTS (
      SELECT 1 FROM {{ ref('com_evolution') }} ce_e
      WHERE ce_e.old_com = g.end_geo_code
        AND ce_e.year = EXTRACT(YEAR FROM g.geo_updated_at)::int
        AND ce_e.mod = 21
    )
),

operators AS (
  SELECT DISTINCT left(siret, 9) AS code, name as libelle
  FROM {{ source('operator', 'operators') }}
),

collectivites AS (
  SELECT DISTINCT code, libelle
  FROM {{ ref('perimeters_agg') }}
  WHERE type in ('epci','aom')
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

carpools AS (
  SELECT
    j._id,
    j.uuid,
    j.legacy_id,
    j.created_at,
    j.updated_at,
    j.operator_id,
    j.operator_name,
    j.operator_siret,
    j.operator_journey_id,
    j.operator_trip_id,
    j.operator_class::varchar AS operator_class,

    j.start_datetime,
    {{ timezoned_ts('COALESCE(geo.start_geo_code, j.start_geo_code)', 'j.start_datetime') }} AS start_datetime_tz,
    h3_lat_lng_to_cell(j.start_position::point, 9) AS start_h3index_z9,
    h3_lat_lng_to_cell(j.start_position::point, 8) AS start_h3index_z8,
    COALESCE(geo.start_geo_code, j.start_geo_code) AS start_geo_code,

    j.end_datetime,
    {{ timezoned_ts('COALESCE(geo.end_geo_code, j.end_geo_code)', 'j.end_datetime') }} AS end_datetime_tz,
    h3_lat_lng_to_cell(j.end_position::point, 9) AS end_h3index_z9,
    h3_lat_lng_to_cell(j.end_position::point, 8) AS end_h3index_z8,
    COALESCE(geo.end_geo_code, j.end_geo_code) AS end_geo_code,

    COALESCE(to_jsonb(geo.geo_errors), to_jsonb(j.geo_errors)) AS geo_errors,
    COALESCE(geo.geo_updated_at, j.geo_updated_at) AS geo_updated_at,

    j.distance,
    j.duration,
    j.driver_key,
    j.driver_revenue,
    j.passenger_key,
    j.passenger_over_18,
    j.passenger_seats,
    j.passenger_contribution,
    j.passenger_payments::jsonb AS passenger_payments,

    COALESCE(oi.oi_amount_collectivite, 0) AS oi_amount_collectivite,
    COALESCE(oi.oi_amount_operator, 0) AS oi_amount_operator,
    COALESCE(oi.oi_amount_other, 0) AS oi_amount_other,
    COALESCE(oi.oi_amount_total, 0) AS oi_amount_total,
    COALESCE(oi.oi_collectivite, 0) AS oi_collectivite,
    COALESCE(oi.oi_operator, 0) AS oi_operator,
    COALESCE(oi.oi_other, 0) AS oi_other,
    (COALESCE(oi.oi_amount_total, 0) > 0) as with_incentive,

    rpc.campaigns,
    rpc.campaigns_amount_total,
    rpc.campaigns_result_total,

    j.fraud_status,
    j.fraud_labels,
    j.anomaly_status,
    j.anomaly_labels,
    j.acquisition_status,
    j.status_updated_at,
    j.final_acquisition_status,
    j.valid_acquisition_status,
    j.terms_violation_error_labels

  FROM base_carpools AS j
  LEFT JOIN geocoding AS geo ON geo.carpool_id = j._id
  LEFT JOIN operator_incentives_agg AS oi ON oi.carpool_id = j._id
  LEFT JOIN campaigns_agg AS rpc ON rpc.carpool_v2_id = j._id
)

SELECT *,
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
FROM carpools
