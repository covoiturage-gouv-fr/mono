MODEL (
  name refined_zone.obs_directions_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date','direction'],
  tags ['refined', 'observatoire', 'directions_by_day'],
);

WITH hours AS (
  SELECT
    code,
    type,
    journey_date,
    direction,
    SUM(journeys)               AS journeys,
    SUM(intra_journeys)         AS intra_journeys,
    SUM(drivers)                AS drivers,
    SUM(passengers)             AS passengers,
    SUM(passenger_seats)        AS passenger_seats,
    SUM(distance)               AS distance,
    SUM(incentive_collectivite) AS incentive_collectivite,
    SUM(incentive_operator)     AS incentive_operator,
    SUM(incentive_others)       AS incentive_others,
    SUM(no_incentive)           AS no_incentive,
    json_agg(
      json_build_object('hour', hour, 'journeys', journeys)
      ORDER BY hour
    ) AS hours_distribution
  FROM refined_zone.obs_directions_hours_by_day
  WHERE code IS NOT NULL
  AND journey_date >= @start_ts
  AND journey_date <  @end_ts
  GROUP BY code, type, journey_date, direction
),
distances AS (
  SELECT
    code,
    type,
    journey_date,
    direction,
    json_agg(
      json_build_object('class', dist_class, 'journeys', journeys)
      ORDER BY dist_class
    ) AS dist_distribution
  FROM refined_zone.obs_directions_distances_by_day
  WHERE code IS NOT NULL
  AND journey_date >= @start_ts
  AND journey_date <  @end_ts
  GROUP BY code, type, journey_date, direction
)
SELECT
  h.code,
  h.type,
  h.journey_date,
  h.direction,
  h.journeys,
  h.intra_journeys,
  h.drivers,
  h.passengers,
  h.passenger_seats,
  h.distance,
  h.incentive_collectivite,
  h.incentive_operator,
  h.incentive_others,
  h.no_incentive,
  h.hours_distribution,
  d.dist_distribution
FROM hours h
LEFT JOIN distances d
  ON  d.code         = h.code
  AND d.type         = h.type
  AND d.journey_date = h.journey_date
  AND d.direction    = h.direction