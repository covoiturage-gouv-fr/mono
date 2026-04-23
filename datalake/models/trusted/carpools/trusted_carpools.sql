{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
    indexes = [
      { 'columns':['_id'] },
      { 'columns':['start_datetime_tz'] },
      { 'columns':['start_h3_index'] },
      { 'columns':['end_h3_index'] },
      { 'columns': ['driver_key'] },
      { 'columns': ['passenger_key'] },
      { 'columns': ['valid_acquisition_status'] }
    ],
    tags=['trusted', 'carpools']
) }}

WITH filtered_carpools AS (
  SELECT
    _id,
    start_position_x,
    start_position_y,
    end_position_x,
    end_position_y
  FROM {{ ref('raw_carpools') }}
  WHERE {{ time_filter('start_datetime', lookback_nb=3) }}
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
  FROM {{ ref('raw_carpools') }} AS g
  INNER JOIN filtered_carpools AS j ON g._id = j._id
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
  FROM {{ ref('raw_carpools') }} AS g
  INNER JOIN filtered_carpools AS j ON g._id = j._id
  LEFT JOIN perimeters_retablissement ps
    ON ST_Intersects(
      ST_SetSRID(ST_Point(j.start_position_x, j.start_position_y), 4326),
      ps.geom
    )
    AND ps.year = EXTRACT(YEAR FROM g.updated_at)::int
  LEFT JOIN perimeters_retablissement pe
    ON ST_Intersects(
      ST_SetSRID(ST_Point(j.end_position_x, j.end_position_y), 4326),
      pe.geom
    )
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
    -- agrégats 
    SUM(oi.amount) FILTER (WHERE c.code IS NOT NULL) AS oi_amount_collectivite,
    SUM(oi.amount) FILTER (WHERE op.code IS NOT NULL) AS oi_amount_operator,
    SUM(oi.amount) FILTER (WHERE c.code IS NULL AND op.code IS NULL AND oi.siret IS NOT NULL) AS oi_amount_other,
    COUNT(*) FILTER (WHERE c.code IS NOT NULL) AS oi_collectivite,
    COUNT(*) FILTER (WHERE op.code IS NOT NULL) AS oi_operator,
    COUNT(*) FILTER (WHERE c.code IS NULL AND op.code IS NULL AND oi.siret IS NOT NULL) AS oi_other,
    SUM(oi.amount) AS oi_amount_total,
    -- détail en jsonb pour les cas où on en a besoin
    jsonb_agg(
      jsonb_build_object(
        'siret', oi.siret, 
        'type', CASE WHEN c.code IS NOT NULL THEN 'collectivite' WHEN op.code IS NOT NULL THEN 'operator' ELSE 'other' END,
        'name', CASE WHEN c.code IS NOT NULL THEN c.libelle WHEN op.code IS NOT NULL THEN op.libelle ELSE NULL END,
        'amount', oi.amount
      )
    ) AS oi_details
  FROM {{ ref('raw_operator_incentives') }} oi
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
  FROM {{ ref('raw_incentives') }}
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
    h3_lat_lng_to_cell(
      (ST_SetSRID(ST_Point(j.start_position_x, j.start_position_y), 4326))::point, 9
    ) AS start_h3_index,
    COALESCE(geo.start_geo_code, j.start_geo_code) AS start_geo_code,

    j.end_datetime,
    {{ timezoned_ts('COALESCE(geo.end_geo_code, j.end_geo_code)', 'j.end_datetime') }} AS end_datetime_tz,
    h3_lat_lng_to_cell(
      (ST_SetSRID(ST_Point(j.end_position_x, j.end_position_y), 4326))::point, 9
    ) AS end_h3_index,
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
    j.valid_acquisition_status

  FROM {{ ref('raw_carpools') }} AS j
  LEFT JOIN geocoding AS geo ON geo.carpool_id = j._id
  LEFT JOIN operator_incentives_agg AS oi ON oi.carpool_id = j._id
  LEFT JOIN campaigns_agg AS rpc ON rpc.carpool_v2_id = j._id

  WHERE {{ time_filter('j.start_datetime', 'start_datetime', lookback_nb=3) }}
)

SELECT *,
EXTRACT('hour' FROM start_datetime_tz)::int AS hour,
CASE
  WHEN distance < 10000 THEN '0-10'
  WHEN distance < 20000 THEN '10-20'
  WHEN distance < 30000 THEN '20-30'
  WHEN distance < 40000 THEN '30-40'
  WHEN distance < 50000 THEN '40-50'
  ELSE '>50'
END AS dist_class,
(start_geo_code = end_geo_code) AS is_intra
FROM carpools 
