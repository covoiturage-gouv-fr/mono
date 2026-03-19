{% macro trusted_journeys_model_generator(source_table, start_ts, end_ts) %}
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
  j.start_datetime_tz,
  ST_Point(j.start_position_x, j.start_position_y) AS start_position,
  j.start_h3_index::h3index AS start_h3_index,
  j.start_geo_code,
  j.end_datetime,
  j.end_datetime_tz,
  ST_Point(j.end_position_x, j.end_position_y) AS end_position,
  j.end_h3_index::h3index AS end_h3_index,
  j.end_geo_code,
  j.geo_errors::jsonb AS geo_errors,
  j.geo_updated_at,
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
  oi.operator_incentives,
  oi.operator_incentives_amount_total,

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

-- Operator incentives with company names.
-- LATERAL join produces both aggregated arrays and a JSONB array [{siret, name, amount}, ...].
LEFT JOIN LATERAL (
  SELECT
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
        'amount',        pi.amount
      )
    ) AS campaign_incentives,
    SUM(pi.amount) AS campaign_incentives_amount_total
  FROM trusted_zone.campaign_incentives pi
  WHERE pi.carpool_v2_id = j._id
) rpc ON TRUE

WHERE j.start_datetime BETWEEN {{start_ts}} AND {{end_ts}}
{% endmacro %}
