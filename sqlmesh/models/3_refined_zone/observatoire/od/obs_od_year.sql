MODEL (
  name refined_zone.obs_od_year,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column year_date,
    batch_size 12,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@monthly',
  grain [ 'year_date', 'type', 'territory_1', 'territory_2'],
  tags ['refined', 'observatoire', 'od_year'],
);

WITH od AS (
  SELECT
    start_geo_code as origin,
    end_geo_code   as destination,
    extract('year' FROM journey_date)::int  AS year,
    sum(journeys)                         AS journeys,
    sum(passenger_seats)                  AS passenger_seats,
    sum(distance)                         AS distance,
    sum(duration)                         AS duration
  FROM refined_zone.obs_od_day
  WHERE journey_date >= @start_ts
    AND journey_date <  @end_ts
  GROUP BY 1, 2, 3
),
-- Pre-resolve perimeters once for all UNION branches
perimeters_resolved AS (
  SELECT DISTINCT ON (p.arr, y.year)
    p.arr, p.epci, p.aom, p.dep, p.reg, p.country, y.year AS j_year
  FROM trusted_zone.perimeters p
  CROSS JOIN (
    SELECT DISTINCT year FROM od
  ) y
  WHERE p.year <= y.year
  ORDER BY p.arr, y.year, p.year DESC
),
-- Pre-resolve AOM regionales once
perimeters_aom_region AS (
  SELECT p.arr, p.aom, ar.aom AS aom_r, p.reg, p.j_year
  FROM perimeters_resolved p
  LEFT JOIN trusted_zone.aom_region ar ON p.reg = ar.reg
),
-- AOM region set for anti-join
aom_region_set AS (
  SELECT aom FROM trusted_zone.aom_region
),
-- Cache perimeters_agg for final joins
perimeters_cache AS (
  SELECT DISTINCT ON (p.code, p.type, y.year)
    p.code, p.type, p.libelle, p.centroid,
    p.year, y.year AS j_year
  FROM trusted_zone.perimeters_agg p
  CROSS JOIN (
    SELECT DISTINCT year FROM od
  ) y
  WHERE p.year <= y.year
  ORDER BY p.code, p.type, y.year, p.year DESC
),
od_agg AS (
  -- com
  SELECT
    year, 'com' AS type,
    least(origin, destination) AS territory_1,
    greatest(origin, destination) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od
  GROUP BY 1, 3, 4
  HAVING least(origin, destination) IS NOT NULL
  UNION
  -- epci
  SELECT
    a.year, 'epci' AS type,
    least(b.epci, c.epci) AS territory_1,
    greatest(b.epci, c.epci) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_resolved b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_resolved c ON c.arr = a.destination AND c.j_year = a.year
  GROUP BY 1, 3, 4
  HAVING least(b.epci, c.epci) IS NOT NULL
  UNION
  -- aom classiques
  SELECT
    a.year, 
    least(b.aom, c.aom) AS territory_1,
    greatest(b.aom, c.aom) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_resolved b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_resolved c ON c.arr = a.destination AND c.j_year = a.year
  LEFT JOIN aom_region_set ar_b ON b.aom = ar_b.aom
  LEFT JOIN aom_region_set ar_c ON c.aom = ar_c.aom
  WHERE ar_b.aom IS NULL OR ar_c.aom IS NULL
  GROUP BY 1, 3, 4
  HAVING least(b.aom, c.aom) IS NOT NULL
  UNION
  -- aom regionales
  SELECT
    a.year, 
    least(b.aom_r, c.aom_r) AS territory_1,
    greatest(b.aom_r, c.aom_r) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_aom_region b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_aom_region c ON c.arr = a.destination AND c.j_year = a.year
  WHERE b.aom <> c.aom
  GROUP BY 1, 3, 4
  HAVING least(b.aom_r, c.aom_r) IS NOT NULL
  UNION
  -- dep
  SELECT
    a.year, 'dep' AS type,
    least(b.dep, c.dep) AS territory_1,
    greatest(b.dep, c.dep) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_resolved b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_resolved c ON c.arr = a.destination AND c.j_year = a.year
  GROUP BY 1, 3, 4
  HAVING least(b.dep, c.dep) IS NOT NULL
  UNION
  -- reg
  SELECT
    a.year, 'reg' AS type,
    least(b.reg, c.reg) AS territory_1,
    greatest(b.reg, c.reg) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_resolved b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_resolved c ON c.arr = a.destination AND c.j_year = a.year
  GROUP BY 1, 3, 4
  HAVING least(b.reg, c.reg) IS NOT NULL
  UNION
  -- country
  SELECT
    a.year, 'country' AS type,
    least(b.country, c.country) AS territory_1,
    greatest(b.country, c.country) AS territory_2,
    sum(journeys) AS journeys,
    sum(passenger_seats) AS passengers,
    round(sum(distance)::numeric / 1000, 2) AS distance,
    sum(duration) / 60 AS duration
  FROM od a
  LEFT JOIN perimeters_resolved b ON b.arr = a.origin AND b.j_year = a.year
  LEFT JOIN perimeters_resolved c ON c.arr = a.destination AND c.j_year = a.year
  GROUP BY 1, 3, 4
  HAVING least(b.country, c.country) IS NOT NULL
)

SELECT
  make_date(a.year, 1, 1) AS year_date,
  a.year,
  a.type,
  a.territory_1,
  b.libelle AS l_territory_1,
  a.territory_2,
  c.libelle AS l_territory_2,
  a.journeys,
  a.passengers,
  a.distance,
  a.duration,
  st_x(b.centroid)  AS lng_1,
  st_y(b.centroid)  AS lat_1,
  st_x(c.centroid)  AS lng_2,
  st_y(c.centroid)  AS lat_2
FROM od_agg AS a
LEFT JOIN perimeters_cache b
  ON  b.code   = a.territory_1
  AND b.type   = a.type
  AND b.j_year = a.year
LEFT JOIN perimeters_cache c
  ON  c.code   = a.territory_2
  AND c.type   = a.type
  AND c.j_year = a.year;

@IF(@runtime_stage = 'creating', CREATE UNIQUE INDEX IF NOT EXISTS uq_year_date_type_territory_1_territory_2 ON refined_zone.obs_od_year (year_date, type, territory_1, territory_2));
