MODEL (
  name refined_zone.export_carpool_list,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    -- Reprocess the last 7 days on each run to catch delayed fraud/anomaly updates
    lookback 7,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  grain '_id',
  tags ['refined', 'export'],
);

-- Latest perimeter labels and position precision per geo code.
-- Uses DISTINCT ON to pick the most recent year for each arr code.
WITH latest_perimeters AS (
  SELECT DISTINCT ON (arr)
    arr,
    dep,
    l_dep,
    epci,
    l_epci,
    aom,
    l_aom,
    reg,
    l_reg,
    -- Country-level geo codes have arr = country; their l_arr is the country name
    CASE WHEN arr <> country THEN l_arr    ELSE NULL  END AS l_arr,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    -- Position precision: 3 decimal places in dense areas (> 40 inh/km²), else 2
    CASE
      WHEN surface > 0 AND (pop::float / (surface / 100.0)) > 40 THEN 3
      ELSE 2
    END AS precision
  FROM trusted_zone.perimeters
  ORDER BY arr, year DESC
)

SELECT
  j._id,
  j.legacy_id                                                    AS journey_id,
  j.operator_trip_id,
  j.operator_journey_id,
  j.operator_class,
  j.acquisition_status,
  j.fraud_status,
  j.anomaly_status,

  -- Raw UTC timestamps.
  -- TZ conversion and rounding (ts_ceil) are done by the API per operator preference.
  j.start_datetime,
  j.end_datetime,
  j.duration,

  -- Distance in km (raw value in trusted_zone is in metres)
  j.distance::float / 1000                                       AS distance,

  -- Coordinates truncated by population density precision
  TRUNC(ST_Y(j.start_position)::numeric, gps.precision)         AS start_lat,
  TRUNC(ST_X(j.start_position)::numeric, gps.precision)         AS start_lon,
  TRUNC(ST_Y(j.end_position)::numeric,   gpe.precision)         AS end_lat,
  TRUNC(ST_X(j.end_position)::numeric,   gpe.precision)         AS end_lon,

  -- Geo codes for API filtering
  j.start_geo_code                                               AS start_insee,
  gps.dep                                                        AS start_dep,
  gps.epci                                                       AS start_epci,
  gps.aom                                                        AS start_aom,
  gps.reg                                                        AS start_reg,

  j.end_geo_code                                                 AS end_insee,
  gpe.dep                                                        AS end_dep,
  gpe.epci                                                       AS end_epci,
  gpe.aom                                                        AS end_aom,
  gpe.reg                                                        AS end_reg,

  -- Geo labels for display
  gps.l_arr                                                      AS start_commune,
  gps.l_dep                                                      AS start_departement,
  gps.l_epci                                                     AS start_epci_name,
  gps.l_aom                                                      AS start_aom_name,
  gps.l_reg                                                      AS start_region,
  gps.l_country                                                  AS start_pays,

  gpe.l_arr                                                      AS end_commune,
  gpe.l_dep                                                      AS end_departement,
  gpe.l_epci                                                     AS end_epci_name,
  gpe.l_aom                                                      AS end_aom_name,
  gpe.l_reg                                                      AS end_region,
  gpe.l_country                                                  AS end_pays,

  -- Operator identity
  j.operator_id,
  j.operator_name                                                AS operator,
  j.passenger_operator_user_id                                   AS operator_passenger_id,
  j.passenger_identity_key,
  j.driver_operator_user_id                                      AS operator_driver_id,
  j.driver_identity_key,

  -- Financial data in euros (raw values in trusted_zone are in cents)
  j.driver_revenue::float / 100                                  AS driver_revenue,
  j.passenger_contribution::float / 100                         AS passenger_contribution,
  j.passenger_seats,

  -- CEE application flag
  cee._id IS NOT NULL                                            AS cee_application,

  -- Operator incentives: JSONB array [{siret, name, amount}, ...] capped at 3 items.
  -- Assembled from operator_incentives_sirets/amounts arrays + company name lookup.
  jsonb_path_query_array(oi.incentive, '$[0 to 2]')             AS incentive,

  -- RPC incentives: JSONB array [{campaign_id, campaign_name, siret, name, amount}, ...] capped at 3 items.
  jsonb_path_query_array(rpc.incentive_rpc, '$[0 to 2]')        AS incentive_rpc,

FROM trusted_zone.journeys j

LEFT JOIN latest_perimeters gps ON j.start_geo_code = gps.arr
LEFT JOIN latest_perimeters gpe ON j.end_geo_code   = gpe.arr

LEFT JOIN cee.cee_applications cee ON cee.carpool_id = j._id

-- Operator incentives with company names.
-- UNNEST pairs sirets and amounts arrays by position (preserves order).
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'siret',  t.siret,
      'name',   c.legal_name,
      'amount', t.amount::float / 100
    ) ORDER BY t.ord
  ) AS incentive
  FROM UNNEST(j.operator_incentives_sirets, j.operator_incentives_amounts)
    WITH ORDINALITY AS t(siret, amount, ord)
  LEFT JOIN company.companies c ON c.siret = t.siret
) oi ON TRUE

-- RPC incentives with campaign and territory details.
-- LATERAL join avoids a full-table scan on policy.incentives.
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'campaign_id',   pp._id,
      'campaign_name', pp.name,
      'siret',         ccp.siret,
      'name',          ttg.name,
      'amount',        pi.amount::float / 100
    )
  ) AS incentive_rpc
  FROM policy.incentives pi
  LEFT JOIN policy.policies pp             ON pi.policy_id    = pp._id
  LEFT JOIN territory.territory_group ttg  ON pp.territory_id = ttg._id
  LEFT JOIN company.companies ccp          ON ttg.company_id  = ccp._id
  WHERE pi.operator_id       = j.operator_id
    AND pi.operator_journey_id = j.operator_journey_id
) rpc ON TRUE

WHERE j.valid_acquisition_status
  AND j.start_datetime BETWEEN @start_ts AND @end_ts;

-- Primary lookup by carpool ID
@create_index(@this_model, _id, 'name=export_carpool_list_id');

-- Operator + time range (most common API filter combination)
@create_index(@this_model, operator_id, start_datetime, 'name=export_carpool_list_operator_date');

-- Geo code filters for start point
@create_index(@this_model, start_dep,  start_datetime, 'name=export_carpool_list_start_dep');
@create_index(@this_model, start_epci, start_datetime, 'name=export_carpool_list_start_epci');
@create_index(@this_model, start_aom,  start_datetime, 'name=export_carpool_list_start_aom');
@create_index(@this_model, start_reg,  start_datetime, 'name=export_carpool_list_start_reg');

-- Geo code filters for end point
@create_index(@this_model, end_dep,  end_datetime, 'name=export_carpool_list_end_dep');
@create_index(@this_model, end_epci, end_datetime, 'name=export_carpool_list_end_epci');
@create_index(@this_model, end_aom,  end_datetime, 'name=export_carpool_list_end_aom');
