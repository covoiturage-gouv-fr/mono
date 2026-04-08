MODEL (
  name trusted_zone.journeys,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain '_id',
  tags ['trusted', 'journeys'],
);

WITH batch AS (
  SELECT
    _id,
    start_position_x,
    start_position_y,
    end_position_x,
    end_position_y
  FROM raw_zone.journeys
  WHERE start_datetime >= @start_ts
    AND start_datetime < @end_ts
),

perimeters_retablissement AS (
  SELECT com, geom
  FROM trusted_zone.perimeters
  WHERE year = EXTRACT(YEAR FROM @start_ts::date)
    AND com IN (
      SELECT DISTINCT new_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM @start_ts::date)
        AND mod = 21
    )
),

geocoding AS (
  -- Cas 1: Communes non fusionnees (mod = 32)
  SELECT
    g.carpool_id,
    COALESCE(cs.new_com, g.start_geo_code) AS start_geo_code,
    COALESCE(ce.new_com, g.end_geo_code) AS end_geo_code,
    g.updated_at AS geo_updated_at,
    g.errors AS geo_errors
  FROM carpool_v2.geo AS g
  INNER JOIN batch AS b ON g.carpool_id = b._id
  LEFT JOIN trusted_zone.com_evolution cs
    ON g.start_geo_code = cs.old_com
    AND cs.mod = 32
    AND cs.year = EXTRACT(YEAR FROM @start_ts::date)
  LEFT JOIN trusted_zone.com_evolution ce
    ON g.end_geo_code = ce.old_com
    AND ce.mod = 32
    AND ce.year = EXTRACT(YEAR FROM @start_ts::date)
  WHERE NOT EXISTS (
      SELECT 1 FROM trusted_zone.com_evolution ce_s
      WHERE ce_s.old_com = g.start_geo_code
        AND ce_s.year = EXTRACT(YEAR FROM @start_ts::date) AND ce_s.mod = 21
    )
    OR NOT EXISTS (
      SELECT 1 FROM trusted_zone.com_evolution ce_e
      WHERE ce_e.old_com = g.end_geo_code
        AND ce_e.year = EXTRACT(YEAR FROM @start_ts::date) AND ce_e.mod = 21
    )

  UNION ALL

  -- Cas 2: Communes fusionnees (mod = 21) - utiliser perimetres
  SELECT
    g.carpool_id,
    COALESCE(ps.com, g.start_geo_code) AS start_geo_code,
    COALESCE(pe.com, g.end_geo_code) AS end_geo_code,
    g.updated_at AS geo_updated_at,
    g.errors AS geo_errors
  FROM carpool_v2.geo g
  INNER JOIN batch AS b ON g.carpool_id = b._id
  LEFT JOIN perimeters_retablissement ps
    ON ST_Intersects(ST_SetSRID(ST_Point(b.start_position_x, b.start_position_y), 4326), ps.geom)
  LEFT JOIN perimeters_retablissement pe
    ON ST_Intersects(ST_SetSRID(ST_Point(b.end_position_x, b.end_position_y), 4326), pe.geom)
  WHERE EXISTS (
      SELECT 1 FROM trusted_zone.com_evolution ce_s
      WHERE ce_s.old_com = g.start_geo_code
        AND ce_s.year = EXTRACT(YEAR FROM @start_ts::date) AND ce_s.mod = 21
    )
    OR EXISTS (
      SELECT 1 FROM trusted_zone.com_evolution ce_e
      WHERE ce_e.old_com = g.end_geo_code
        AND ce_e.year = EXTRACT(YEAR FROM @start_ts::date) AND ce_e.mod = 21
    )
),

-- Pre-aggregate operator incentives (replaces LATERAL)
operator_incentives_agg AS (
  SELECT
    carpool_id,
    ARRAY_AGG(DISTINCT siret) AS operator_incentives_sirets,
    ARRAY_AGG(DISTINCT amount) AS operator_incentives_amounts,
    SUM(amount) AS operator_incentives_amount_total,
    jsonb_agg(
      jsonb_build_object(
        'siret', siret,
        'name', legal_name,
        'amount', amount
      ) ORDER BY siret
    ) AS operator_incentives
  FROM raw_zone.operator_incentives
  WHERE start_datetime >= @start_ts
    AND start_datetime < @end_ts
  GROUP BY carpool_id
),

-- Pre-aggregate campaign incentives (replaces LATERAL + UNION ALL view)
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
  FROM raw_zone.incentives
  WHERE datetime >= @start_ts
    AND datetime < @end_ts
  GROUP BY carpool_v2_id
),

journeys AS (
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
    @get_timezoned_timestamp(COALESCE(geo.start_geo_code, j.start_geo_code), j.start_datetime) AS start_datetime_tz,
    ST_SetSRID(ST_Point(j.start_position_x, j.start_position_y), 4326) AS start_position,
    h3_lat_lng_to_cell((ST_SetSRID(ST_Point(j.start_position_x, j.start_position_y), 4326))::point, 9) AS start_h3_index,
    COALESCE(geo.start_geo_code, j.start_geo_code) AS start_geo_code,

    j.end_datetime,
    @get_timezoned_timestamp(COALESCE(geo.end_geo_code, j.end_geo_code), j.end_datetime) AS end_datetime_tz,
    ST_SetSRID(ST_Point(j.end_position_x, j.end_position_y), 4326) AS end_position,
    h3_lat_lng_to_cell((ST_SetSRID(ST_Point(j.end_position_x, j.end_position_y), 4326))::point, 9) AS end_h3_index,
    COALESCE(geo.end_geo_code, j.end_geo_code) AS end_geo_code,

    COALESCE(to_jsonb(geo.geo_errors), to_jsonb(j.geo_errors)) AS geo_errors,
    COALESCE(geo.geo_updated_at, j.geo_updated_at) AS geo_updated_at,

    j.distance,
    j.duration,
    j.licence_plate,
    j.driver_identity_key,
    j.driver_operator_user_id,
    j.driver_phone,
    j.driver_phone_trunc,
    j.driver_id,
    j.driver_travelpass_name,
    j.driver_travelpass_user_id,
    j.driver_revenue,
    j.passenger_identity_key,
    j.passenger_operator_user_id,
    j.passenger_phone,
    j.passenger_phone_trunc,
    j.passenger_id,
    j.passenger_travelpass_name,
    j.passenger_travelpass_user_id,
    j.passenger_over_18,
    j.passenger_seats,
    j.passenger_contribution,
    j.passenger_payments::jsonb AS passenger_payments,

    -- operator incentives
    oi.operator_incentives_sirets,
    oi.operator_incentives_amounts,
    oi.operator_incentives_amount_total,
    oi.operator_incentives,

    -- RPC incentives
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

  FROM raw_zone.journeys AS j

  LEFT JOIN geocoding AS geo ON geo.carpool_id = j._id
  LEFT JOIN operator_incentives_agg AS oi ON oi.carpool_id = j._id
  LEFT JOIN campaigns_agg AS rpc ON rpc.carpool_v2_id = j._id

  WHERE j.start_datetime >= @start_ts
    AND j.start_datetime < @end_ts
)

SELECT * FROM journeys;

@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS journeys_id_index ON trusted_zone.journeys (_id));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS journeys_start_datetime_tz_index ON trusted_zone.journeys (start_datetime_tz));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS journeys_start_h3_idx ON trusted_zone.journeys (start_h3_index));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS journeys_end_h3_idx ON trusted_zone.journeys (end_h3_index));
