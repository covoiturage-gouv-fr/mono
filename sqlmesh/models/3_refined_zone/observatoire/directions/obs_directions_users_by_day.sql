MODEL (
  name refined_zone.obs_directions_users_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date','direction'],
  tags ['refined', 'observatoire', 'directions_users_by_day'],
);

WITH journeys_enriched AS (

  SELECT
    j._id,
    j.driver_id,
    j.passenger_id,
    j.start_geo_code,
    j.end_geo_code,
    j.start_datetime_tz::date AS journey_date,
    CASE
      WHEN u_driver.first_date_driver = j.start_datetime_tz::date
      THEN TRUE ELSE FALSE
    END AS is_new_driver,
    CASE
      WHEN u_passenger.first_date_passenger = j.start_datetime_tz::date
      THEN TRUE ELSE FALSE
    END AS is_new_passenger
  FROM trusted_zone.journeys j
  LEFT JOIN refined_zone.obs_users u_driver ON j.driver_id = u_driver.user_id
  LEFT JOIN refined_zone.obs_users u_passenger ON j.passenger_id = u_passenger.user_id
  WHERE j.valid_acquisition_status = true
    AND j.start_datetime_tz >= @start_ds
    AND j.start_datetime_tz <  @end_ds
),
journey_years AS (
  SELECT EXTRACT(YEAR FROM @start_ds::date)::int AS year
  UNION
  SELECT EXTRACT(YEAR FROM @end_ds::date)::int
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
journeys AS (
  SELECT
    a._id,
    a.driver_id,
    a.passenger_id,
    a.journey_date,
    a.is_new_driver,
    a.is_new_passenger,
    ps.epci AS start_epci,
    pe.epci AS end_epci,
    ps.aom  AS start_aom,
    pe.aom  AS end_aom,
    ps.dep  AS start_dep,
    pe.dep  AS end_dep,
    ps.reg  AS start_reg,
    pe.reg  AS end_reg,
    ps.country AS start_country,
    pe.country AS end_country
  FROM journeys_enriched a
  LEFT JOIN perimeters_resolved ps
    ON ps.arr = a.start_geo_code
    AND ps.journey_year = EXTRACT(YEAR FROM a.journey_date)
  LEFT JOIN perimeters_resolved pe
    ON pe.arr = a.end_geo_code
    AND pe.journey_year = EXTRACT(YEAR FROM a.journey_date)
),
-- Directions pour tous les territoires SAUF AOM
directions_non_aom AS (
  SELECT
    j.journey_date,
    d.direction,
    d.type,
    d.start_code as code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger,
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
    d.start_code AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger,
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
  WHERE d.start_code NOT IN (SELECT aom FROM trusted_zone.aom_region)
),
-- Directions pour AOM régionales
directions_aom_regional AS (
  SELECT
    j.journey_date,
    'from' AS direction,
    'aom' AS type,
    ar.aom AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger,
    CASE 
      WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom 
      THEN TRUE 
      ELSE FALSE 
    END AS is_intra
  FROM journeys j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)  -- Exclut les intra-AOM
  
  UNION ALL
  
  SELECT
    j.journey_date,
    'to' AS direction,
    'aom' AS type,
    ar.aom AS code,
    j.driver_id,
    j.passenger_id,
    j.is_new_driver,
    j.is_new_passenger,
    CASE 
      WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom 
      THEN TRUE 
      ELSE FALSE 
    END AS is_intra
  FROM journeys j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.end_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)  -- Exclut les intra-AOM
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
  direction,
  COUNT(DISTINCT driver_id) AS unique_drivers,
  COUNT(DISTINCT passenger_id) AS unique_passengers,
  COUNT(DISTINCT CASE
    WHEN is_new_driver THEN driver_id END
  ) AS new_drivers,
  COUNT(DISTINCT CASE
    WHEN is_new_passenger THEN passenger_id END
  ) AS new_passengers
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4
UNION ALL
SELECT
  code,
  type,
  journey_date,
  'both' AS direction,
  COUNT(DISTINCT driver_id) AS unique_drivers,
  COUNT(DISTINCT passenger_id) AS unique_passengers,
  COUNT(DISTINCT CASE
    WHEN is_new_driver THEN driver_id END
  ) AS new_drivers,
  COUNT(DISTINCT CASE
    WHEN is_new_passenger THEN passenger_id END
  ) AS new_passengers
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3

