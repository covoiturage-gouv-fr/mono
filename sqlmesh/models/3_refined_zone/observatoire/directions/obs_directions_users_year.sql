MODEL (
  name refined_zone.obs_directions_users_year,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column year_date,
    lookback 1,
    batch_size 12,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@monthly',
  grain ['code','type','year_date','direction'],
  tags ['refined', 'observatoire', 'directions_users_year'],
);

WITH journey_years AS (
  SELECT EXTRACT(YEAR FROM @start_ts::date)::int AS year
  UNION
  SELECT EXTRACT(YEAR FROM @end_ts::date)::int
),
perimeters_resolved AS (
  SELECT DISTINCT ON (p.arr, y.year)
    p.arr,
    p.epci,
    p.aom,
    p.dep,
    p.reg,
    p.country,
    y.year AS journey_year
  FROM trusted_zone.perimeters p
  CROSS JOIN journey_years y
  WHERE p.year <= y.year
  ORDER BY p.arr, y.year, p.year DESC
),
journeys_enriched AS (
  SELECT
    j._id,
    j.driver_id,
    j.passenger_id,
    DATE_TRUNC('year', j.start_datetime)::date AS year_date,
    ps.epci    AS start_epci,
    pe.epci    AS end_epci,
    ps.aom     AS start_aom,
    pe.aom     AS end_aom,
    ps.dep     AS start_dep,
    pe.dep     AS end_dep,
    ps.reg     AS start_reg,
    pe.reg     AS end_reg,
    ps.country AS start_country,
    pe.country AS end_country,
    j.start_geo_code AS start_com,
    j.end_geo_code   AS end_com,
    CASE WHEN u_driver.first_date_driver       = j.start_datetime::date THEN TRUE ELSE FALSE END AS is_new_driver,
    CASE WHEN u_passenger.first_date_passenger = j.start_datetime::date THEN TRUE ELSE FALSE END AS is_new_passenger
  FROM trusted_zone.journeys j
  LEFT JOIN perimeters_resolved ps
         ON ps.arr = j.start_geo_code
        AND ps.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
  LEFT JOIN perimeters_resolved pe
         ON pe.arr = j.end_geo_code
        AND pe.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
  LEFT JOIN refined_zone.obs_users u_driver    ON u_driver.user_id    = j.driver_id
  LEFT JOIN refined_zone.obs_users u_passenger ON u_passenger.user_id = j.passenger_id
  WHERE j.valid_acquisition_status = true
    AND j.start_datetime >= @start_ts
    AND j.start_datetime <  @end_ts
),
directions_non_aom AS (
  SELECT
    j.year_date,
    d.direction,
    d.type,
    d.start_code AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from','com',     j.start_com,     j.start_com),
      ('to',  'com',     j.end_com,       j.end_com),
      ('from','epci',    j.start_epci,    j.start_epci),
      ('to',  'epci',    j.end_epci,      j.end_epci),
      ('from','dep',     j.start_dep,     j.start_dep),
      ('to',  'dep',     j.end_dep,       j.end_dep),
      ('from','reg',     j.start_reg,     j.start_reg),
      ('to',  'reg',     j.end_reg,       j.end_reg),
      ('from','country', j.start_country, j.start_country),
      ('to',  'country', j.end_country,   j.end_country)
  ) AS d(direction, type, start_code, end_code)
),
directions_aom_classic AS (
  SELECT
    j.year_date,
    d.direction,
    d.type,
    d.start_code AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from','aom', j.start_aom, j.end_aom),
      ('to',  'aom', j.end_aom,   j.start_aom)
  ) AS d(direction, type, start_code, end_code)
  WHERE d.start_code NOT IN (SELECT aom FROM trusted_zone.aom_region)
),
directions_aom_regional AS (
  SELECT
    j.year_date,
    'from' AS direction,
    'aom'  AS type,
    ar.aom AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)

  UNION ALL

  SELECT
    j.year_date,
    'to'   AS direction,
    'aom'  AS type,
    ar.aom AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.end_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)
),
all_directions AS (
  SELECT * FROM directions_non_aom
  UNION ALL
  SELECT * FROM directions_aom_classic
  UNION ALL
  SELECT * FROM directions_aom_regional
)
SELECT
  code,
  type,
  year_date,
  direction,
  COUNT(DISTINCT driver_id)                                        AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                     AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4

UNION ALL

SELECT
  code,
  type,
  year_date,
  'both'                                                           AS direction,
  COUNT(DISTINCT driver_id)                                        AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                     AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM all_directions
WHERE code IS NOT NULL
  AND direction = 'from'
GROUP BY 1,2,3;

@create_unique_index(@this_model, year_date, code, type, direction);