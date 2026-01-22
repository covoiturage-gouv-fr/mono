MODEL (
  name refined_zone.obs_od_semester,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column semester_date
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
  FROM refined_zone.obs_journeys_by_day
  WHERE journey_date >= @start_ts
    AND journey_date <  @end_ts
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
  HAVING least(origin, destination) IS NOT NULL OR greatest(origin, destination) IS NOT NULL
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
  LEFT JOIN
    (
      SELECT
        arr,
        epci
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS b
    ON a.origin = b.arr
  LEFT JOIN
    (
      SELECT
        arr,
        epci
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS c
    ON a.destination = c.arr
  GROUP BY 1, 2, 4, 5
  HAVING
    least(b.epci, c.epci) IS NOT NULL
    OR greatest(b.epci, c.epci) IS NOT NULL
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
  LEFT JOIN
    (
      SELECT
        arr,
        aom
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS b
    ON a.origin = b.arr
  LEFT JOIN
    (
      SELECT
        arr,
        aom
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS c
    ON a.destination = c.arr
  GROUP BY 1, 2, 4, 5
  HAVING least(b.aom, c.aom) IS NOT NULL OR greatest(b.aom, c.aom) IS NOT NULL
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
  LEFT JOIN
    (
      SELECT
        arr,
        dep
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS b
    ON a.origin = b.arr
  LEFT JOIN
    (
      SELECT
        arr,
        dep
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS c
    ON a.destination = c.arr
  GROUP BY 1, 2, 4, 5
  HAVING least(b.dep, c.dep) IS NOT NULL OR greatest(b.dep, c.dep) IS NOT NULL
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
  LEFT JOIN
    (
      SELECT
        arr,
        reg
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS b
    ON a.origin = b.arr
  LEFT JOIN
    (
      SELECT
        arr,
        reg
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS c
    ON a.destination = c.arr
  GROUP BY 1, 2, 4, 5
  HAVING least(b.reg, c.reg) IS NOT NULL OR greatest(b.reg, c.reg) IS NOT NULL
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
  LEFT JOIN
    (
      SELECT
        arr,
        country
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS b
    ON a.origin = b.arr
  LEFT JOIN
    (
      SELECT
        arr,
        country
      FROM trusted_zone.perimeters
      WHERE year = geo.get_latest_millesime()
    ) AS c
    ON a.destination = c.arr
  GROUP BY 1, 2, 4, 5
  HAVING
    least(b.country, c.country) IS NOT NULL
    OR greatest(b.country, c.country) IS NOT NULL
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
LEFT JOIN
  (
    SELECT
      code,
      type,
      libelle,
      centroid
    FROM trusted_zone.perimeters_agg
    WHERE year = geo.get_latest_millesime()
    UNION
    SELECT
      code,
      'com',
      libelle,
      centroid
    FROM trusted_zone.perimeters_agg
    WHERE
      year = geo.get_latest_millesime()
      AND type = 'country'
  ) AS b
  ON concat(a.territory_1, a.type) = concat(b.code, b.type)
LEFT JOIN
  (
    SELECT
      code,
      type,
      libelle,
      centroid
    FROM trusted_zone.perimeters_agg
    WHERE year = geo.get_latest_millesime()
    UNION
    SELECT
      code,
      'com',
      libelle,
      centroid
    FROM trusted_zone.perimeters_agg
    WHERE
      year = geo.get_latest_millesime()
      AND type = 'country'
  ) AS c
  ON concat(a.territory_2, a.type) = concat(c.code, c.type)
ORDER BY a.territory_1, a.territory_2