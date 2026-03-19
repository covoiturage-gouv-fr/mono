{% macro trusted_journeys_model_generator(source_table, start_ts, end_ts) %}
WITH batch AS (
  SELECT
    _id,
    start_position_x,
    start_position_y,
    end_position_x,
    end_position_y
  FROM {{source_table}}
  WHERE start_datetime BETWEEN {{start_ts}} AND {{end_ts}}
),

perimeters_retablissement AS (
  SELECT com, geom
  FROM trusted_zone.perimeters
  WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date)
    AND com IN (
      SELECT DISTINCT new_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date)
        AND mod = 21
    )
),

geocoding AS (
  -- Cas 1: Communes non fusionnées (mod = 32)
  SELECT
    g.carpool_id,
    COALESCE(cs.new_com, g.start_geo_code) AS start_geo_code,
    COALESCE(ce.new_com, g.end_geo_code)   AS end_geo_code,
    g.updated_at                           AS geo_updated_at,
    g.errors                               AS geo_errors
  FROM carpool_v2.geo AS g
  INNER JOIN batch AS b ON g.carpool_id = b._id
  LEFT JOIN trusted_zone.com_evolution cs
    ON g.start_geo_code = cs.old_com
    AND cs.mod = 32
    AND cs.year = EXTRACT(YEAR FROM {{start_ts}}::date)
  LEFT JOIN trusted_zone.com_evolution ce
    ON g.end_geo_code = ce.old_com
    AND ce.mod = 32
    AND ce.year = EXTRACT(YEAR FROM {{start_ts}}::date)
  WHERE g.start_geo_code NOT IN (
      SELECT DISTINCT old_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date) AND mod = 21
    )
    OR g.end_geo_code NOT IN (
      SELECT DISTINCT old_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date) AND mod = 21
    )

  UNION ALL

  -- Cas 2: Communes fusionnées (mod = 21) - utiliser perimetres
  SELECT
    g.carpool_id,
    COALESCE(ps.com, g.start_geo_code) AS start_geo_code,
    COALESCE(pe.com, g.end_geo_code)   AS end_geo_code,
    g.updated_at                       AS geo_updated_at,
    g.errors                           AS geo_errors
  FROM carpool_v2.geo g
  INNER JOIN batch AS b ON g.carpool_id = b._id
  LEFT JOIN perimeters_retablissement ps
    ON ST_Intersects(ST_Point(b.start_position_x, b.start_position_y)::geometry, ps.geom)
  LEFT JOIN perimeters_retablissement pe
    ON ST_Intersects(ST_Point(b.end_position_x, b.end_position_y)::geometry, pe.geom)
  WHERE g.start_geo_code IN (
      SELECT DISTINCT old_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date) AND mod = 21
    )
    OR g.end_geo_code IN (
      SELECT DISTINCT old_com
      FROM trusted_zone.com_evolution
      WHERE year = EXTRACT(YEAR FROM {{start_ts}}::date) AND mod = 21
    )
)

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
  {{get_timezoned_timestamp("COALESCE(geo.start_geo_code, j.start_geo_code)", "j.start_datetime")}} AS start_datetime_tz,
  ST_Point(j.start_position_x, j.start_position_y) AS start_position,
  j.start_h3_index::h3index AS start_h3_index,
  COALESCE(geo.start_geo_code, j.start_geo_code) AS start_geo_code,

  j.end_datetime,
  {{get_timezoned_timestamp("COALESCE(geo.end_geo_code, j.end_geo_code)", "j.end_datetime")}} AS end_datetime_tz,
  ST_Point(j.end_position_x, j.end_position_y) AS end_position,
  j.end_h3_index::h3index AS end_h3_index,
  COALESCE(geo.end_geo_code, j.end_geo_code) AS end_geo_code,

  COALESCE(geo.geo_errors, j.geo_errors)::jsonb AS geo_errors,
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
  rpc.campaign_incentives,
  rpc.campaign_incentives_amount_total,
  rpc.campaign_incentives_result_total,

  j.fraud_status,
  j.fraud_labels,
  j.anomaly_status,
  j.anomaly_labels,
  j.acquisition_status,
  j.status_updated_at,
  j.final_acquisition_status,
  j.valid_acquisition_status

FROM {{source_table}} AS j

LEFT JOIN geocoding AS geo ON geo.carpool_id = j._id

-- Operator incentives with company names.
-- LATERAL join produces both aggregated arrays and a JSONB array [{siret, name, amount}, ...].
LEFT JOIN LATERAL (
  SELECT
    ARRAY_AGG(DISTINCT t.siret)  AS operator_incentives_sirets,
    ARRAY_AGG(DISTINCT t.amount) AS operator_incentives_amounts,
    SUM(t.amount)                AS operator_incentives_amount_total,
    jsonb_agg(
      jsonb_build_object(
        'siret',  t.siret,
        'name',   comp.legal_name,
        'amount', t.amount
      ) ORDER BY t.siret
    )                            AS operator_incentives
  FROM carpool_v2.operator_incentives t
  LEFT JOIN company.companies comp ON comp.siret = t.siret
  WHERE t.carpool_id = j._id
    AND t.amount > 0
) oi ON TRUE

-- RPC incentives with campaign and territory details.
-- LATERAL join avoids a full-table scan on trusted_zone.campaign_incentives.
LEFT JOIN LATERAL (
  SELECT
    jsonb_agg(
      jsonb_build_object(
        'campaign_id',   pi.campaign_id,
        'campaign_name', pi.campaign_name,
        'siret',         pi.territory_siret,
        'name',          pi.territory_name,
        'amount',        pi.amount,
        'result',        pi.result
      )
    ) AS campaign_incentives,
    SUM(pi.amount) AS campaign_incentives_amount_total,
    SUM(pi.result) AS campaign_incentives_result_total
  FROM trusted_zone.campaign_incentives pi
  WHERE pi.carpool_v2_id = j._id
) rpc ON TRUE

WHERE j.start_datetime BETWEEN {{start_ts}} AND {{end_ts}}
{% endmacro %}
