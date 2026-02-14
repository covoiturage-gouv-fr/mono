MODEL (
  name refined_zone.obs_od_semester,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column semester_date,
    batch_size 6,
  ),
  start '2020-01-01',
  cron '@monthly',
  grain [ 'semester_date', 'type', 'territory_1', 'territory_2'],
  tags ['refined', 'observatoire', 'od_semester'],
);

WITH od AS (
  SELECT
    start_geo_code as origin,
    end_geo_code   as destination,
    extract('year' FROM journey_date)::int  AS year,  
    ceil(extract('month' FROM journey_date)::int / 6.0)::int AS semester,
    sum(journeys)                         AS journeys,
    sum(passenger_seats)                  AS passenger_seats,
    sum(distance)                         AS distance,
    sum(duration)                         AS duration
  FROM refined_zone.obs_od_day
  WHERE journey_date >= @start_ds
    AND journey_date <  @end_ds
  GROUP BY
    1, 2, 3, 4
),
od_agg AS (
  SELECT
    year,
    semester,
    'com'                                    AS type,
    least(origin, destination)                      AS territory_1,
    greatest(origin, destination)                   AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od
  GROUP BY 1, 2, 4, 5
  HAVING least(origin, destination) IS NOT NULL
  UNION
  SELECT
    a.year,
    a.semester,
    'epci'                                   AS type,
    least(b.epci, c.epci)                    AS territory_1,
    greatest(b.epci, c.epci)                 AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT year, arr, epci
    FROM trusted_zone.perimeters 
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT year, arr, epci
    FROM trusted_zone.perimeters
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  GROUP BY 1, 2, 4, 5
  HAVING least(b.epci, c.epci) IS NOT NULL
  /* aom classiques */
  UNION
  SELECT
    a.year,
    a.semester,
    'aom'                                    AS type,
    least(b.aom, c.aom)                      AS territory_1,
    greatest(b.aom, c.aom)                   AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT year, arr, aom
    FROM trusted_zone.perimeters 
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT year, arr, aom
    FROM trusted_zone.perimeters
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  WHERE b.aom NOT IN (SELECT aom FROM trusted_zone.aom_region)
    OR c.aom NOT IN (SELECT aom FROM trusted_zone.aom_region)
  GROUP BY 1, 2, 4, 5
  HAVING least(b.aom, c.aom) IS NOT NULL
   /* aom région */
  UNION
  SELECT
    a.year,
    a.semester,
    'aom'                                    AS type,
    least(b.aom_r, c.aom_r)                  AS territory_1,
    greatest(b.aom_r, c.aom_r)               AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT p.year, p.arr, p.aom, ar.aom AS aom_r, p.reg
    FROM trusted_zone.perimeters p
    LEFT JOIN trusted_zone.aom_region ar ON p.reg = ar.reg
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT p.year, p.arr, p.aom, ar.aom AS aom_r, p.reg
    FROM trusted_zone.perimeters p
    LEFT JOIN trusted_zone.aom_region ar ON p.reg = ar.reg
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  WHERE b.aom <> c.aom
  GROUP BY 1, 2, 4, 5
  HAVING least(b.aom_r, c.aom_r) IS NOT NULL
  UNION
  SELECT
    a.year,
    a.semester,
    'dep'                                    AS type,
    least(b.dep, c.dep)                      AS territory_1,
    greatest(b.dep, c.dep)                   AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT year, arr, dep
    FROM trusted_zone.perimeters 
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT year, arr, dep
    FROM trusted_zone.perimeters
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  GROUP BY 1, 2, 4, 5
  HAVING least(b.dep, c.dep) IS NOT NULL
  UNION
  SELECT
    a.year,
    a.semester,
    'reg'                                    AS type,
    least(b.reg, c.reg)                      AS territory_1,
    greatest(b.reg, c.reg)                   AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT year, arr, reg
    FROM trusted_zone.perimeters 
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT year, arr, reg
    FROM trusted_zone.perimeters
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  GROUP BY 1, 2, 4, 5
  HAVING least(b.reg, c.reg) IS NOT NULL
  UNION
  SELECT
    a.year,
    a.semester,
    'country'                                AS type,
    least(b.country, c.country)              AS territory_1,
    greatest(b.country, c.country)           AS territory_2,
    sum(journeys)                            AS journeys,
    sum(passenger_seats)                     AS passengers,
    round(sum(distance)::numeric / 1000, 2)  AS distance,
    sum(duration) / 60 AS duration
  FROM od AS a
  LEFT JOIN (
    SELECT year, arr, country
    FROM trusted_zone.perimeters 
  ) b ON  b.arr = a.origin and b.year = @get_millesime_or_latest(a.year::smallint)
  LEFT JOIN (
    SELECT year, arr, country
    FROM trusted_zone.perimeters
  ) c ON c.arr = a.destination and c.year = @get_millesime_or_latest(a.year::smallint)
  GROUP BY 1, 2, 4, 5
  HAVING least(b.country, c.country) IS NOT NULL
)

SELECT
  make_date(a.year, (a.semester-1)*6 + 1, 1) AS semester_date,
  a.year,
  a.semester,
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
LEFT JOIN @join_perimeters_agg(@start_ds, @end_ds) AS b
  ON  b.code   = a.territory_1
  AND b.type   = a.type
  AND b.j_year = a.year
LEFT JOIN @join_perimeters_agg(@start_ds, @end_ds) AS c
  ON  c.code   = a.territory_2
  AND c.type   = a.type
  AND c.j_year = a.year