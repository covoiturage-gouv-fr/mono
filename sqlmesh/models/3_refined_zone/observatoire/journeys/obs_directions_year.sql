MODEL (
  name refined_zone.obs_directions_year,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column year_date
  ),
  start '2020-01-01',
  cron '@monthly',
  grain [ 'year_date', 'code', 'type', 'direction'],
  tags ['refined', 'observatoire', 'directions_year'],
);

WITH sum_directions AS (
  SELECT
    code,
    type,   
    direction,
    extract('year' FROM journey_date)::int  AS year,
    sum(journeys)                         AS journeys,
    (sum(distance) / sum(journeys)) * sum(passenger_seats) AS passengers_distance,
    (sum(distance) / sum(journeys)) * sum(drivers) AS drivers_distance,
    sum(incentive_collectivite)       AS incentive_collectivite,
    sum(incentive_operator)          AS incentive_operator,
    sum(incentive_others)            AS incentive_others
  FROM refined_zone.obs_directions_by_day
  WHERE journey_date >= @start_ts
    AND journey_date <  @end_ts
  GROUP BY 1, 2, 3, 4
  HAVING sum(journeys) > 0
)

SELECT
  make_date(a.year, 1, 1) AS year_date,
  a.year,
  a.code,
  b.libelle,
  a.type,
  a.direction,
  a.journeys,
  a.incentive_collectivite,
  a.incentive_operator,
  a.incentive_others,
  round(
    (drivers_distance + passengers_distance) / drivers_distance, 2
  )::float                      AS occupation_rate,
  st_asgeojson(b.centroid, 6)::json AS geom
FROM sum_directions AS a
LEFT JOIN LATERAL
(
  SELECT
    code,
    type,
    libelle,
    centroid
  FROM trusted_zone.perimeters_agg AS p
  WHERE p.code = a.code
    AND p.type = a.type
    AND p.year <= a.year
  ORDER BY p.year DESC
  LIMIT 1
) AS b ON TRUE
WHERE
  a.code IS NOT null
  AND a.drivers_distance > 0
  AND a.passengers_distance > 0
ORDER BY 1, 2, 3, 4, 5, 6