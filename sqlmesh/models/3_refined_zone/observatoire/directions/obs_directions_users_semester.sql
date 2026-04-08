MODEL (
  name refined_zone.obs_directions_users_semester,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column semester_date,
    lookback 1,
    batch_size 6,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@monthly',
  grain ['code','type','semester_date','direction'],
  tags ['refined', 'observatoire', 'directions_users_semester'],
);

WITH journeys_enriched AS (
  SELECT
    j._id,
    j.driver_id,
    j.passenger_id,
    DATE_TRUNC('month', j.start_datetime_tz -
      INTERVAL '1 month' * ((EXTRACT(MONTH FROM j.start_datetime_tz)::int - 1) % 6)
    )::date AS semester_date,
    g.start_epci,
    g.end_epci,
    g.start_aom,
    g.end_aom,
    g.start_dep,
    g.end_dep,
    g.start_reg,
    g.end_reg,
    g.start_country,
    g.end_country,
    j.start_geo_code AS start_com,
    j.end_geo_code   AS end_com,
    CASE WHEN u_driver.first_date_driver       = j.start_datetime_tz::date THEN TRUE ELSE FALSE END AS is_new_driver,
    CASE WHEN u_passenger.first_date_passenger = j.start_datetime_tz::date THEN TRUE ELSE FALSE END AS is_new_passenger
  FROM trusted_zone.journeys j
  LEFT JOIN refined_zone.obs_journeys_geo g ON g._id = j._id
  LEFT JOIN refined_zone.obs_users u_driver    ON u_driver.user_id    = j.driver_id
  LEFT JOIN refined_zone.obs_users u_passenger ON u_passenger.user_id = j.passenger_id
  WHERE j.valid_acquisition_status = true
    AND j.start_datetime >= @start_ts
    AND j.start_datetime <  @end_ts
),
directions_non_aom AS (
  SELECT
    j.semester_date,
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
    j.semester_date,
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
    j.semester_date,
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
    j.semester_date,
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
  semester_date,
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
  semester_date,
  'both'                                                           AS direction,
  COUNT(DISTINCT driver_id)                                        AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                     AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3;

@IF(@runtime_stage = 'creating', CREATE UNIQUE INDEX IF NOT EXISTS uq_semester_date_code_type_direction ON refined_zone.obs_directions_users_semester (semester_date, code, type, direction));