MODEL (
  name refined_zone.obs_directions_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 3,
    batch_size 1,
    batch_concurrency 1,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain ['code', 'type', 'journey_date', 'direction'],
  tags ['refined', 'observatoire', 'directions_day'],
);

SET work_mem = '512MB';

WITH journeys_enriched AS (
  SELECT
    j._id,
    j.driver_id,
    j.passenger_id,
    j.start_datetime_tz::date AS journey_date,
    EXTRACT('hour' FROM j.start_datetime_tz)::int AS hour,
    j.passenger_seats,
    j.distance,
    CASE
      WHEN j.distance < 10000 THEN '0-10'
      WHEN j.distance < 20000 THEN '10-20'
      WHEN j.distance < 30000 THEN '20-30'
      WHEN j.distance < 40000 THEN '30-40'
      WHEN j.distance < 50000 THEN '40-50'
      ELSE '>50'
    END AS dist_class,
    -- Geographie (pre-computed)
    g.start_epci, g.start_aom, g.start_dep, g.start_reg, g.start_country,
    j.start_geo_code AS start_com,
    g.end_epci, g.end_aom, g.end_dep, g.end_reg, g.end_country,
    j.end_geo_code AS end_com,
    -- Incentives (pre-computed)
    COALESCE(i.incentive_collectivite, 0) AS incentive_collectivite,
    COALESCE(i.incentive_operator, 0) AS incentive_operator,
    COALESCE(i.incentive_others, 0) AS incentive_others,
    COALESCE(i.no_incentive, 0) AS no_incentive,
    -- Nouveaux utilisateurs
    (ud.first_date_driver = j.start_datetime_tz::date) AS is_new_driver,
    (up.first_date_passenger = j.start_datetime_tz::date) AS is_new_passenger
  FROM trusted_zone.journeys j
  LEFT JOIN refined_zone.obs_journeys_geo g ON g._id = j._id
  LEFT JOIN refined_zone.obs_journeys_incentives i ON i._id = j._id
  LEFT JOIN refined_zone.obs_users ud ON ud.user_id = j.driver_id
  LEFT JOIN refined_zone.obs_users up ON up.user_id = j.passenger_id
  WHERE j.start_datetime_tz >= @start_ts
    AND j.start_datetime_tz < @end_ts
    AND j.valid_acquisition_status = true
),

-- Explosion geo
directions_exploded AS (
  -- Dimensions non-AOM (com, epci, dep, reg, country)
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    d.direction, d.type, d.code,
    (d.code IS NOT NULL AND d.code = d.end_code) AS is_intra
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from','com',     j.start_com,     j.end_com),
      ('to',  'com',     j.end_com,       j.start_com),
      ('from','epci',    j.start_epci,    j.end_epci),
      ('to',  'epci',    j.end_epci,      j.start_epci),
      ('from','dep',     j.start_dep,     j.end_dep),
      ('to',  'dep',     j.end_dep,       j.start_dep),
      ('from','reg',     j.start_reg,     j.end_reg),
      ('to',  'reg',     j.end_reg,       j.start_reg),
      ('from','country', j.start_country, j.end_country),
      ('to',  'country', j.end_country,   j.start_country)
  ) AS d(direction, type, code, end_code)
  WHERE d.code IS NOT NULL

  UNION ALL

  -- AOM classiques (hors AOM regionales)
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    d.direction, 'aom' AS type, d.code,
    (d.code IS NOT NULL AND d.code = d.end_code) AS is_intra
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from', j.start_aom, j.end_aom),
      ('to',   j.end_aom,   j.start_aom)
  ) AS d(direction, code, end_code)
  WHERE d.code IS NOT NULL
    AND d.code NOT IN (SELECT aom FROM trusted_zone.aom_region)

  UNION ALL

  -- AOM regionales from
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    'from' AS direction, 'aom' AS type, ar.aom AS code,
    (j.start_reg = j.end_reg AND j.start_aom <> j.end_aom) AS is_intra
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)

  UNION ALL

  -- AOM regionales to
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    'to' AS direction, 'aom' AS type, ar.aom AS code,
    (j.start_reg = j.end_reg AND j.start_aom <> j.end_aom) AS is_intra
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.end_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)
),

-- Niveau 1 : grain (code, type, journey_date, direction, hour, dist_class)
-- from/to agrege depuis l'explosion geo
agg_from_to AS (
  SELECT
    code, type, journey_date, hour, dist_class, direction,
    COUNT(DISTINCT _id) AS journeys,
    COUNT(DISTINCT _id) FILTER (WHERE is_intra) AS intra_journeys,
    COUNT(DISTINCT driver_id) AS drivers,
    COUNT(DISTINCT passenger_id) AS passengers,
    COUNT(DISTINCT CASE WHEN is_new_driver THEN driver_id END) AS new_drivers,
    COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers,
    SUM(passenger_seats) AS passenger_seats,
    SUM(distance) AS distance,
    SUM(incentive_collectivite) AS incentive_collectivite,
    SUM(incentive_operator) AS incentive_operator,
    SUM(incentive_others) AS incentive_others,
    SUM(no_incentive) AS no_incentive
  FROM directions_exploded
  GROUP BY 1, 2, 3, 4, 5, 6
),
-- "both" construit depuis from/to pre-agreges (evite un 2e scan de l'explosion)
agg_detail AS (
  SELECT * FROM agg_from_to

  UNION ALL

  SELECT
    code, type, journey_date, hour, dist_class,
    'both' AS direction,
    SUM(journeys) AS journeys,
    SUM(intra_journeys) AS intra_journeys,
    SUM(drivers) AS drivers,
    SUM(passengers) AS passengers,
    SUM(new_drivers) AS new_drivers,
    SUM(new_passengers) AS new_passengers,
    SUM(passenger_seats)
      - COALESCE(SUM(passenger_seats) FILTER (WHERE direction = 'from'), 0) AS passenger_seats,
    SUM(distance)
      - COALESCE(SUM(distance) FILTER (WHERE direction = 'from'), 0) AS distance,
    SUM(incentive_collectivite)
      - COALESCE(SUM(incentive_collectivite) FILTER (WHERE direction = 'from'), 0) AS incentive_collectivite,
    SUM(incentive_operator)
      - COALESCE(SUM(incentive_operator) FILTER (WHERE direction = 'from'), 0) AS incentive_operator,
    SUM(incentive_others)
      - COALESCE(SUM(incentive_others) FILTER (WHERE direction = 'from'), 0) AS incentive_others,
    SUM(no_incentive)
      - COALESCE(SUM(no_incentive) FILTER (WHERE direction = 'from'), 0) AS no_incentive
  FROM agg_from_to
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  d.code,
  d.type,
  d.journey_date,
  d.direction,
  SUM(d.journeys) AS journeys,
  SUM(d.intra_journeys) AS intra_journeys,
  SUM(d.drivers) AS drivers,
  SUM(d.passengers) AS passengers,
  SUM(d.new_drivers) AS new_drivers,
  SUM(d.new_passengers) AS new_passengers,
  SUM(d.passenger_seats) AS passenger_seats,
  SUM(d.distance) AS distance,
  SUM(d.incentive_collectivite) AS incentive_collectivite,
  SUM(d.incentive_operator) AS incentive_operator,
  SUM(d.incentive_others) AS incentive_others,
  SUM(d.no_incentive) AS no_incentive,
  ARRAY(
    SELECT COALESCE(SUM(d2.journeys), 0)
    FROM generate_series(0,23) AS h
    LEFT JOIN agg_detail d2
      ON d2.code = d.code
      AND d2.type = d.type
      AND d2.journey_date = d.journey_date
      AND d2.direction = d.direction
      AND d2.hour = h
    GROUP BY h
    ORDER BY h
  ) AS hours_distribution,
  ARRAY(
    SELECT COALESCE(SUM(d2.journeys),0)
    FROM (SELECT unnest(ARRAY['0-10','10-20','20-30','30-40','40-50','>50']) AS dist_class) dc
    LEFT JOIN agg_detail d2
      ON d2.code = d.code
      AND d2.type = d.type
      AND d2.journey_date = d.journey_date
      AND d2.direction = d.direction
      AND d2.dist_class = dc.dist_class
    GROUP BY dc.dist_class
    ORDER BY dc.dist_class
  ) AS dist_distribution
FROM agg_detail as d
GROUP BY 1, 2, 3, 4;

@create_indexes(
  'UNIQUE uq_code_type_journey_date_direction ON refined_zone.obs_directions_day (code, type, journey_date, direction)',
  'obs_directions_day_date_index ON refined_zone.obs_directions_day (journey_date)',
);
