MODEL (
  name refined_zone.obs_directions_hours_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date','hour','direction'],
  tags ['refined', 'observatoire', 'directions_hours_by_day'],
);

WITH journeys AS (
  SELECT
    a.journey_date,
    a.start_geo_code AS start_com,
    a.end_geo_code   AS end_com,
    ps.epci AS start_epci,
    pe.epci AS end_epci,
    ps.aom AS start_aom,
    pe.aom AS end_aom,
    ps.dep AS start_dep,
    pe.dep AS end_dep,
    ps.reg AS start_reg,
    pe.reg AS end_reg,
    ps.country AS start_country,
    pe.country AS end_country,
    a.journeys,
    a.drivers,
    a.passengers,
    a.passenger_seats,
    a.distance,
    a.duration,
    a.incentive_collectivite,
    a.incentive_operator,
    a.incentive_others,
    a.no_incentive
  FROM refined_zone.obs_journeys_by_day AS a
  LEFT JOIN LATERAL (
    SELECT arr, epci, aom, dep, reg, country
    FROM trusted_zone.perimeters p
    WHERE p.arr = a.start_geo_code
      AND p.year <= EXTRACT(YEAR FROM a.journey_date)
    ORDER BY p.year DESC
    LIMIT 1
  ) ps ON TRUE
  LEFT JOIN LATERAL (
    SELECT arr, epci, aom, dep, reg, country
    FROM trusted_zone.perimeters p
    WHERE p.arr = a.end_geo_code
      AND p.year <= EXTRACT(YEAR FROM a.journey_date)
    ORDER BY p.year DESC
    LIMIT 1
  ) pe ON TRUE
  WHERE a.journey_date BETWEEN @start_ds AND @end_ds
),
aom_region AS (
  SELECT *
  FROM (
    VALUES
      ('200053767', '84'),
      ('200053742', '32'),
      ('231300021', '93'),
      ('200053726', '27'),
      ('233500016', '53'),
      ('234500023', '24'),
      ('200076958', '94'),
      ('229850003', '06'),
      ('200052264', '44'),
      ('239730013', '03'),
      ('200053403', '28'),
      ('200053759', '75'),
      ('200053791', '76'),
      ('234400034', '52')
  ) AS t(code, reg)
),
-- Directions pour tous les territoires SAUF AOM
directions_non_aom AS (
  SELECT
    j.journey_date,
    d.direction,
    d.type,
    d.start_code as code,
    j.journeys,
    j.drivers,
    j.passengers,
    j.passenger_seats,
    j.distance,
    j.incentive_collectivite,
    j.incentive_operator,
    j.incentive_others,
    j.no_incentive,
    CASE 
      WHEN intra_start IS NOT NULL 
       AND intra_start = intra_end
      THEN TRUE ELSE FALSE 
    END AS is_intra
  FROM journeys j
  CROSS JOIN LATERAL (
    VALUES
      -- COMMUNE
      ('from','com', start_com, end_com,   start_com, end_com),
      ('to',  'com', end_com,   start_com, end_com,   start_com),
      -- EPCI
      ('from','epci', start_epci, end_epci, start_epci, end_epci),
      ('to',  'epci', end_epci,   start_epci, end_epci,   start_epci),
      -- DEP
      ('from','dep', start_dep, end_dep, start_dep, end_dep),
      ('to',  'dep', end_dep,   start_dep, end_dep,   start_dep),
      -- REG
      ('from','reg', start_reg, end_reg, start_reg, end_reg),
      ('to',  'reg', end_reg,   start_reg, end_reg,   start_reg),
      -- COUNTRY
      ('from','country', start_country, end_country, start_country, end_country),
      ('to',  'country', end_country,   start_country, end_country,   start_country)
  ) AS d(direction, type, start_code, end_code, intra_start, intra_end)
),
-- Directions pour AOM classiques (hors AOM régionales)
directions_aom_classic AS (
  SELECT
    j.journey_date,
    d.direction,
    d.type,
    d.start_code as code,
    j.journeys,
    j.drivers,
    j.passengers,
    j.passenger_seats,
    j.distance,
    j.incentive_collectivite,
    j.incentive_operator,
    j.incentive_others,
    j.no_incentive,
    CASE 
      WHEN intra_start IS NOT NULL 
       AND intra_start = intra_end
      THEN TRUE ELSE FALSE 
    END AS is_intra
  FROM journeys j
  CROSS JOIN LATERAL (
    VALUES
      ('from','aom', start_aom, end_aom, start_aom, end_aom),
      ('to',  'aom', end_aom,   start_aom, end_aom,   start_aom)
  ) AS d(direction, type, start_code, end_code, intra_start, intra_end)
  WHERE d.start_code NOT IN (SELECT code FROM aom_region)
),
-- Directions pour AOM régionales
directions_aom_regional AS (
  SELECT
    j.journey_date,
    'from' AS direction,
    'aom' AS type,
    ar.code,
    j.journeys,
    j.drivers,
    j.passengers,
    j.passenger_seats,
    j.distance,
    j.incentive_collectivite,
    j.incentive_operator,
    j.incentive_others,
    j.no_incentive,
    CASE 
      WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom 
      THEN TRUE 
      ELSE FALSE 
    END AS is_intra
  FROM journeys j
  INNER JOIN aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.code)  -- Exclut les intra-AOM
  
  UNION ALL
  
  SELECT
    j.journey_date,
    'to' AS direction,
    'aom' AS type,
    ar.code,
    j.journeys,
    j.drivers,
    j.passengers,
    j.passenger_seats,
    j.distance,
    j.incentive_collectivite,
    j.incentive_operator,
    j.incentive_others,
    j.no_incentive,
    CASE 
      WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom 
      THEN TRUE 
      ELSE FALSE 
    END AS is_intra
  FROM journeys j
  INNER JOIN aom_region ar ON ar.reg = j.end_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.code)  -- Exclut les intra-AOM
),
-- Union de toutes les directions
all_directions AS (
  SELECT * FROM directions_non_aom
  UNION ALL
  SELECT * FROM directions_aom_classic
  UNION ALL
  SELECT * FROM directions_aom_regional
)
-- Agrégations finales
SELECT
  code,
  type,
  journey_date,
  extract('hour' from journey_date) AS hour,
  direction,
  SUM(journeys) AS journeys,
  COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS intra_journeys,
  SUM(drivers) AS drivers,
  SUM(passengers) AS passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(distance) AS distance,
  SUM(incentive_collectivite) AS incentive_collectivite,
  SUM(incentive_operator) AS incentive_operator,
  SUM(incentive_others) AS incentive_others,
  SUM(no_incentive) AS no_incentive
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4,5

UNION ALL

-- Direction 'both'
SELECT
  code,
  type,
  journey_date,
  extract('hour' from journey_date) AS hour,
  'both' AS direction,
  SUM(journeys) - COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS journeys,
  COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS intra_journeys,
  SUM(drivers) - COALESCE(SUM(drivers) FILTER (WHERE is_intra = TRUE), 0) AS drivers,
  SUM(passengers) - COALESCE(SUM(passengers) FILTER (WHERE is_intra = TRUE), 0) AS passengers,
  SUM(passenger_seats) - COALESCE(SUM(passenger_seats) FILTER (WHERE is_intra = TRUE), 0) AS passenger_seats,
  SUM(distance) - COALESCE(SUM(distance) FILTER (WHERE is_intra = TRUE), 0) AS distance,
  SUM(incentive_collectivite) - COALESCE(SUM(incentive_collectivite) FILTER (WHERE is_intra = TRUE), 0) AS incentive_collectivite,
  SUM(incentive_operator) - COALESCE(SUM(incentive_operator) FILTER (WHERE is_intra = TRUE), 0) AS incentive_operator,
  SUM(incentive_others) - COALESCE(SUM(incentive_others) FILTER (WHERE is_intra = TRUE), 0) AS incentive_others,
  SUM(no_incentive) - COALESCE(SUM(no_incentive) FILTER (WHERE is_intra = TRUE), 0) AS no_incentive
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX IF NOT EXISTS obs_directions_hours_by_day_pk ON refined_zone.obs_directions_hours_by_day (code, type, journey_date, hour, direction);