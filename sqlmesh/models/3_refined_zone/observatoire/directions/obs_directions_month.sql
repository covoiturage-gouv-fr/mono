MODEL (
  name refined_zone.obs_directions_month,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column month_date,
    batch_size 3,
  ),
  start '2020-01-01',
  cron '@monthly',
  grain [ 'month_date', 'code', 'type', 'direction'],
  tags ['refined', 'observatoire', 'directions_month'],
);

WITH sum_directions AS (
  SELECT
    code,
    type,   
    direction,
    extract('year' FROM journey_date)::int  AS year,
    extract('month' FROM journey_date)::int AS month,
    sum(journeys)                         AS journeys,
    (sum(distance) / sum(journeys)) * sum(passenger_seats) AS passengers_distance,
    (sum(distance) / sum(journeys)) * sum(drivers) AS drivers_distance,
    sum(incentive_collectivite)       AS incentive_collectivite,
    sum(incentive_operator)          AS incentive_operator,
    sum(incentive_others)            AS incentive_others,
    SUM(no_incentive)                AS no_incentive
  FROM refined_zone.obs_directions_day
  WHERE journey_date >= @start_ds
    AND journey_date <  @end_ds
  GROUP BY 1, 2, 3, 4, 5
  HAVING sum(journeys) > 0
),
hours_agg AS (
  SELECT
    code,
    type,
    direction,
    extract('year'  FROM journey_date)::int AS year,
    extract('month' FROM journey_date)::int AS month,
    jsonb_object_agg(hour::text, journeys ORDER BY hour) AS hours_distribution
  FROM refined_zone.obs_directions_hours_day
  WHERE journey_date >= @start_ds
    AND journey_date <  @end_ds
  GROUP BY 1, 2, 3, 4, 5
),
dist_agg AS (
  SELECT
    code,
    type,
    direction,
    extract('year'  FROM journey_date)::int AS year,
    extract('month' FROM journey_date)::int AS month,
    jsonb_object_agg(dist_class::text, journeys ORDER BY dist_class) AS dist_distribution
  FROM refined_zone.obs_directions_distances_day
  WHERE journey_date >= @start_ds
    AND journey_date <  @end_ds
  GROUP BY 1, 2, 3, 4, 5
),
users as (
  SELECT
    code,
    type,
    direction,
    extract('year'  FROM month_date)::int AS year,
    extract('month' FROM month_date)::int AS month,
    unique_drivers,
    unique_passengers,
    new_drivers,
    new_passengers
  FROM refined_zone.obs_directions_users_month
  WHERE month_date >= @start_ds
    AND month_date <  @end_ds
)

SELECT
  make_date(a.year, a.month, 1) AS month_date,
  a.year,
  a.month,
  a.code,
  b.libelle,
  a.type,
  a.direction,
  a.journeys,
  a.incentive_collectivite,
  a.incentive_operator,
  a.incentive_others,
  a.no_incentive,
  round(
    (drivers_distance + passengers_distance) / drivers_distance, 2
  )::float                      AS occupation_rate,
  h.hours_distribution,
  d.dist_distribution,
  u.unique_drivers,
  u.unique_passengers,
  u.new_drivers,
  u.new_passengers,
  st_asgeojson(b.centroid, 6)::json AS geom
FROM sum_directions AS a
LEFT JOIN hours_agg h
  ON  h.code      = a.code
  AND h.type      = a.type
  AND h.direction = a.direction
  AND h.year      = a.year
  AND h.month     = a.month
LEFT JOIN dist_agg d
  ON  d.code      = a.code
  AND d.type      = a.type
  AND d.direction = a.direction
  AND d.year      = a.year
  AND d.month     = a.month
LEFT JOIN users u
  ON  u.code      = a.code
  AND u.type      = a.type
  AND u.direction = a.direction
  AND u.year      = a.year
  AND u.month     = a.month
LEFT JOIN @join_perimeters_agg(@start_ds, @end_ds) AS b
  ON  b.code    = a.code
  AND b.type    = a.type
  AND b.j_year = a.year
WHERE
  a.code IS NOT null
  AND a.drivers_distance > 0
  AND a.passengers_distance > 0
ORDER BY 1, 2, 3, 4, 5, 6, 7;

@create_unique_index(@this_model, month_date, code, type, direction);