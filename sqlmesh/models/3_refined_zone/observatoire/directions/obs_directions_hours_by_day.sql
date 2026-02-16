MODEL (
  name refined_zone.obs_directions_hours_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date','hour','direction'],
  tags ['refined', 'observatoire', 'directions_hours_by_day'],
);

WITH unnested_sirets AS (
  SELECT
    a._id,
    a.start_geo_code,
    a.end_geo_code,
    a.start_datetime_tz,
    s.siret,
    CASE 
      WHEN d.code IS NOT NULL
        THEN 'collectivite'
      WHEN c.siren IS NOT NULL
        THEN 'operator'
      WHEN d.code is null and c.siren IS null and s.siret is not null
        THEN 'other'
      ELSE NULL
    END AS siret_type
  FROM trusted_zone.journeys a
  LEFT JOIN LATERAL (SELECT unnest(a.operator_incentives_sirets) AS siret) s ON TRUE
  LEFT JOIN (select distinct left(siret,9) as siren from operator.operators) c ON left(s.siret,9) = c.siren
  LEFT JOIN (select distinct code FROM trusted_zone.perimeters_agg WHERE type = 'aom') d ON left(s.siret,9) = d.code
  WHERE a.valid_acquisition_status = true
  AND start_datetime_tz >= @start_ds
  AND start_datetime_tz <  @end_ds
),
incentives_agg AS (
  SELECT
    start_geo_code AS start_com,
    end_geo_code AS end_com,
    extract('hour' from start_datetime_tz) AS hour,
    start_datetime_tz::date AS journey_date,
    COUNT(*) FILTER (WHERE siret_type = 'collectivite') AS incentive_collectivite,
    COUNT(*) FILTER (WHERE siret_type = 'operator')     AS incentive_operator,
    COUNT(*) FILTER (WHERE siret_type = 'other')        AS incentive_others,
    COUNT(*) FILTER (WHERE siret_type IS NULL)           AS no_incentive
  FROM unnested_sirets
  GROUP BY 1,2,3,4
),
journeys_agg AS (
  SELECT 
    extract('hour' from start_datetime_tz) AS hour,
    start_datetime_tz::date AS journey_date,
    start_geo_code AS start_com,
    end_geo_code   AS end_com,
    count(DISTINCT _id)::int AS journeys,
    count(DISTINCT driver_id)::int AS drivers,
    count(DISTINCT passenger_id)::int AS passengers,
    sum(passenger_seats)::int AS passenger_seats,
    sum(distance)::int AS distance,
    sum(duration) AS duration
  FROM trusted_zone.journeys 
  WHERE valid_acquisition_status = true
    AND start_datetime_tz >= @start_ds
    AND start_datetime_tz <  @end_ds
  GROUP BY 1,2,3,4
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
  a.hour,
  a.journey_date,
  a.start_com,
  a.end_com,
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
  b.incentive_collectivite,
  b.incentive_operator,
  b.incentive_others,
  b.no_incentive
FROM journeys_agg a
LEFT JOIN incentives_agg b
  ON  a.start_com    = b.start_com
  AND a.end_com      = b.end_com
  AND a.hour         = b.hour
  AND a.journey_date = b.journey_date
LEFT JOIN perimeters_resolved ps
  ON ps.arr          = a.start_com
  AND ps.journey_year = EXTRACT(YEAR FROM a.journey_date)::int
LEFT JOIN perimeters_resolved pe
  ON pe.arr          = a.end_com
  AND pe.journey_year = EXTRACT(YEAR FROM a.journey_date)::int
),
-- Directions pour tous les territoires SAUF AOM
directions_non_aom AS (
  SELECT
    j.journey_date,
    j.hour,
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
    j.hour,
    d.direction,
    d.type,
    d.start_code AS code,
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
  WHERE d.start_code NOT IN (SELECT aom FROM trusted_zone.aom_region)
),
-- Directions pour AOM régionales
directions_aom_regional AS (
  SELECT
    j.journey_date,
    j.hour,
    'from' AS direction,
    'aom' AS type,
    ar.aom AS code,
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
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)  -- Exclut les intra-AOM
  
  UNION ALL
  
  SELECT
    j.journey_date,
    j.hour,
    'to' AS direction,
    'aom' AS type,
    ar.aom AS code,
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
  hour,
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
  hour,
  'both' AS direction,
  SUM(journeys) - COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS journeys,
  COALESCE(SUM(journeys) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS intra_journeys,
  SUM(drivers) - COALESCE(SUM(drivers) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS drivers,
  SUM(passengers) - COALESCE(SUM(passengers) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS passengers,
  SUM(passenger_seats) - COALESCE(SUM(passenger_seats) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS passenger_seats,
  SUM(distance) - COALESCE(SUM(distance) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS distance,
  SUM(incentive_collectivite) - COALESCE(SUM(incentive_collectivite) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS incentive_collectivite,
  SUM(incentive_operator) - COALESCE(SUM(incentive_operator) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS incentive_operator,
  SUM(incentive_others) - COALESCE(SUM(incentive_others) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS incentive_others,
  SUM(no_incentive) - COALESCE(SUM(no_incentive) FILTER (WHERE is_intra = TRUE AND direction = 'from'), 0) AS no_incentive
FROM all_directions
WHERE code IS NOT NULL
GROUP BY 1,2,3,4;

@create_unique_index(@this_model, code, type, journey_date, hour, direction);