MODEL (
  name refined_zone.export_carpool_list,
  kind VIEW,
  tags ['refined', 'export'],
);

WITH latest_perimeters AS (
  SELECT DISTINCT ON (arr)
    arr,
    com,
    dep,
    l_dep,
    epci,
    l_epci,
    aom,
    l_aom,
    reg,
    l_reg,
    country,
    CASE WHEN arr <> country THEN l_arr    ELSE NULL  END AS l_arr,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    CASE
      WHEN surface > 0 AND (pop::float / (surface / 100.0)) > 40 THEN 3
      ELSE 2
    END AS precision
  FROM trusted_zone.perimeters
  ORDER BY arr, year DESC
)

SELECT
  j.legacy_id                                                    AS journey_id,
  j.operator_trip_id,
  j.operator_journey_id,
  j.operator_class,
  j.acquisition_status,
  j.fraud_status,
  j.anomaly_status,
  j.operator_id,

  -- date filtering column (raw DATE in Europe/Paris)
  j.start_datetime_tz::date                                     AS start_date_filter,

  -- formatted dates (rounded to 10 min, Europe/Paris)
  to_char(ts_ceil(j.start_datetime_tz, 600), 'YYYY-MM-DD HH24:MI:SS') AS start_datetime,
  to_char(ts_ceil(j.start_datetime_tz, 600), 'YYYY-MM-DD')            AS start_date,
  to_char(ts_ceil(j.start_datetime_tz, 600), 'HH24:MI:SS')            AS start_time,
  to_char(ts_ceil(j.end_datetime_tz,   600), 'YYYY-MM-DD HH24:MI:SS') AS end_datetime,
  to_char(ts_ceil(j.end_datetime_tz,   600), 'YYYY-MM-DD')            AS end_date,
  to_char(ts_ceil(j.end_datetime_tz,   600), 'HH24:MI:SS')            AS end_time,
  to_char((j.duration || ' seconds')::interval, 'HH24:MI:SS')   AS duration,

  j.distance::float / 1000                                       AS distance,

  TRUNC(ST_Y(j.start_position)::numeric, gps.precision)         AS start_lat,
  TRUNC(ST_X(j.start_position)::numeric, gps.precision)         AS start_lon,
  TRUNC(ST_Y(j.end_position)::numeric,   gpe.precision)         AS end_lat,
  TRUNC(ST_X(j.end_position)::numeric,   gpe.precision)         AS end_lon,

  j.start_geo_code                                               AS start_insee,
  gps.arr                                                        AS start_arr,
  gps.com                                                        AS start_com,
  gps.dep                                                        AS start_dep,
  gps.epci                                                       AS start_epci_code,
  gps.aom                                                        AS start_aom_code,
  gps.reg                                                        AS start_reg,
  gps.country                                                    AS start_country,

  j.end_geo_code                                                 AS end_insee,
  gpe.arr                                                        AS end_arr,
  gpe.com                                                        AS end_com,
  gpe.dep                                                        AS end_dep,
  gpe.epci                                                       AS end_epci_code,
  gpe.aom                                                        AS end_aom_code,
  gpe.reg                                                        AS end_reg,
  gpe.country                                                    AS end_country,

  gps.l_arr                                                      AS start_commune,
  gps.l_dep                                                      AS start_departement,
  gps.l_epci                                                     AS start_epci,
  gps.l_aom                                                      AS start_aom,
  gps.l_reg                                                      AS start_region,
  gps.l_country                                                  AS start_pays,

  gpe.l_arr                                                      AS end_commune,
  gpe.l_dep                                                      AS end_departement,
  gpe.l_epci                                                     AS end_epci,
  gpe.l_aom                                                      AS end_aom,
  gpe.l_reg                                                      AS end_region,
  gpe.l_country                                                  AS end_pays,

  j.operator_name                                                AS operator,
  j.passenger_operator_user_id                                   AS operator_passenger_id,
  j.passenger_identity_key,
  j.driver_operator_user_id                                      AS operator_driver_id,
  j.driver_identity_key,

  j.driver_revenue::float / 100                                  AS driver_revenue,
  j.passenger_contribution::float / 100                          AS passenger_contribution,
  j.passenger_seats,

  cee.cee_application_id IS NOT NULL                              AS cee_application,

  -- operator incentives (spread from JSONB array)
  j.operator_incentives[0]->>'siret'                             AS incentive_0_siret,
  j.operator_incentives[0]->>'name'                              AS incentive_0_name,
  j.operator_incentives[0]->>'amount'                            AS incentive_0_amount,

  j.operator_incentives[1]->>'siret'                             AS incentive_1_siret,
  j.operator_incentives[1]->>'name'                              AS incentive_1_name,
  j.operator_incentives[1]->>'amount'                            AS incentive_1_amount,

  j.operator_incentives[2]->>'siret'                             AS incentive_2_siret,
  j.operator_incentives[2]->>'name'                              AS incentive_2_name,
  j.operator_incentives[2]->>'amount'                            AS incentive_2_amount,

  -- RPC incentives (spread from JSONB array)
  j.campaign_incentives[0]->>'campaign_id'                       AS incentive_rpc_0_campaign_id,
  j.campaign_incentives[0]->>'campaign_name'                     AS incentive_rpc_0_campaign_name,
  j.campaign_incentives[0]->>'siret'                             AS incentive_rpc_0_siret,
  j.campaign_incentives[0]->>'name'                              AS incentive_rpc_0_name,
  j.campaign_incentives[0]->>'amount'                            AS incentive_rpc_0_amount,

  j.campaign_incentives[1]->>'campaign_id'                       AS incentive_rpc_1_campaign_id,
  j.campaign_incentives[1]->>'campaign_name'                     AS incentive_rpc_1_campaign_name,
  j.campaign_incentives[1]->>'siret'                             AS incentive_rpc_1_siret,
  j.campaign_incentives[1]->>'name'                              AS incentive_rpc_1_name,
  j.campaign_incentives[1]->>'amount'                            AS incentive_rpc_1_amount,

  j.campaign_incentives[2]->>'campaign_id'                       AS incentive_rpc_2_campaign_id,
  j.campaign_incentives[2]->>'campaign_name'                     AS incentive_rpc_2_campaign_name,
  j.campaign_incentives[2]->>'siret'                             AS incentive_rpc_2_siret,
  j.campaign_incentives[2]->>'name'                              AS incentive_rpc_2_name,
  j.campaign_incentives[2]->>'amount'                            AS incentive_rpc_2_amount,

  -- offer (placeholders)
  TRUE                                                           AS offer_public,
  j.start_datetime                                               AS offer_accepted_at

FROM trusted_zone.journeys j

LEFT JOIN latest_perimeters gps ON j.start_geo_code = gps.arr
LEFT JOIN latest_perimeters gpe ON j.end_geo_code   = gpe.arr
LEFT JOIN trusted_zone.cee_grouped_journeys cee ON cee.carpool_v2_id = j._id

WHERE j.valid_acquisition_status = TRUE
