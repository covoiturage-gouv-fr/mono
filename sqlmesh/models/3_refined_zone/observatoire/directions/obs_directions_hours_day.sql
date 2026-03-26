MODEL (
  name refined_zone.obs_directions_hours_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  grain ['code','type','journey_date','hour','direction'],
  tags ['refined', 'observatoire', 'directions_hours_day'],
);

SELECT
  code,
  type,
  journey_date,
  hour,
  direction,
  COUNT(DISTINCT _id)                         AS journeys,
  COUNT(DISTINCT _id) FILTER (WHERE is_intra) AS intra_journeys,
  COUNT(DISTINCT driver_id)                   AS drivers,
  COUNT(DISTINCT passenger_id)                AS passengers,
  SUM(passenger_seats)                        AS passenger_seats,
  SUM(distance)                               AS distance,
  SUM(incentive_collectivite)                 AS incentive_collectivite,
  SUM(incentive_operator)                     AS incentive_operator,
  SUM(incentive_others)                       AS incentive_others,
  SUM(no_incentive)                           AS no_incentive
FROM trusted_zone.journeys_directions
WHERE code IS NOT NULL
  AND journey_date >= @start_ts::date
  AND journey_date <  @end_ts::date
GROUP BY 1,2,3,4,5

UNION ALL

SELECT
  code,
  type,
  journey_date,
  hour,
  'both'                                      AS direction,
  COUNT(DISTINCT _id)                         AS journeys,
  COUNT(DISTINCT _id) FILTER (WHERE is_intra) AS intra_journeys,
  COUNT(DISTINCT driver_id)                   AS drivers,
  COUNT(DISTINCT passenger_id)                AS passengers,
  SUM(passenger_seats)                        AS passenger_seats,
  SUM(distance)                               AS distance,
  SUM(incentive_collectivite)                 AS incentive_collectivite,
  SUM(incentive_operator)                     AS incentive_operator,
  SUM(incentive_others)                       AS incentive_others,
  SUM(no_incentive)                           AS no_incentive
FROM trusted_zone.journeys_directions
WHERE code IS NOT NULL
  AND direction = 'from'
  AND journey_date >= @start_ts::date
  AND journey_date <  @end_ts::date
GROUP BY 1,2,3,4;

@create_unique_index(@this_model, code, type, journey_date, hour, direction);