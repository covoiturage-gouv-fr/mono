MODEL (
  name refined_zone.obs_directions_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date'],
  tags ['refined', 'observatoire', 'directions_by_day'],
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
    a.duration
  FROM refined_zone.obs_journeys_by_day AS a
  LEFT JOIN trusted_zone.perimeters AS ps ON a.start_geo_code = ps.arr AND ps.year = EXTRACT(YEAR FROM a.journey_date)
  LEFT JOIN trusted_zone.perimeters AS pe ON a.end_geo_code = pe.arr AND pe.year = EXTRACT(YEAR FROM a.journey_date)
  WHERE a.journey_date BETWEEN @start_ds AND @end_ds
),
directions AS (
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
      -- AOM
      ('from','aom', start_aom, end_aom, start_aom, end_aom),
      ('to',  'aom', end_aom,   start_aom, end_aom,   start_aom),
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
)
SELECT
  code,
  type,
  journey_date,
  direction,
  SUM(journeys) AS journeys,
  COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS intra_journeys,
  SUM(drivers) AS drivers,
  SUM(passengers) AS passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(distance) AS distance
FROM directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4
UNION ALL
SELECT
  code,
  type,
  journey_date,
  'both' AS direction,
  SUM(journeys) - COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS journeys,
  COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE), 0) AS intra_journeys,
  SUM(drivers) - COALESCE(SUM(drivers) FILTER (WHERE is_intra = TRUE), 0) AS drivers,
  SUM(passengers) - COALESCE(SUM(passengers) FILTER (WHERE is_intra = TRUE), 0) AS passengers,
  SUM(passenger_seats) - COALESCE(SUM(passenger_seats) FILTER (WHERE is_intra = TRUE), 0) AS passenger_seats,
  SUM(distance) - COALESCE(SUM(distance) FILTER (WHERE is_intra = TRUE), 0) AS distance
FROM directions
WHERE code IS NOT NULL
GROUP BY 1,2,3;

CREATE UNIQUE INDEX IF NOT EXISTS obs_directions_by_day_pk ON refined_zone.obs_directions_by_day (code, type, journey_date, direction);
