MODEL (
  name refined_zone.obs_od_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain ['start_geo_code','end_geo_code','journey_date'],
  tags ['refined', 'observatoire', 'journeys_by_day'],
);

WITH incentives_agg AS (
  SELECT
    j.start_geo_code,
    j.end_geo_code,
    j.start_datetime_tz::date AS journey_date,
    SUM(i.incentive_collectivite) AS incentive_collectivite,
    SUM(i.incentive_operator) AS incentive_operator,
    SUM(i.incentive_others) AS incentive_others,
    SUM(i.no_incentive) AS no_incentive
  FROM trusted_zone.journeys j
  LEFT JOIN refined_zone.obs_journeys_incentives i ON i._id = j._id
  WHERE j.valid_acquisition_status = true
    AND j.start_datetime_tz BETWEEN @start_ts AND @end_ts
  GROUP BY 1, 2, 3
),

journeys_agg AS (
  SELECT
    start_geo_code,
    end_geo_code,
    start_datetime_tz::date AS journey_date,
    count(DISTINCT _id)::int AS journeys,
    count(DISTINCT driver_id)::int AS drivers,
    count(DISTINCT passenger_id)::int AS passengers,
    sum(passenger_seats)::int AS passenger_seats,
    sum(distance)::int AS distance,
    sum(duration) AS duration
  FROM trusted_zone.journeys
  WHERE valid_acquisition_status = true
    AND start_datetime_tz BETWEEN @start_ts AND @end_ts
  GROUP BY 1, 2, 3
)

SELECT
  j.start_geo_code,
  j.end_geo_code,
  j.journey_date,
  j.journeys,
  j.drivers,
  j.passengers,
  j.passenger_seats,
  j.distance,
  j.duration,
  COALESCE(i.incentive_collectivite, 0) AS incentive_collectivite,
  COALESCE(i.incentive_operator, 0)     AS incentive_operator,
  COALESCE(i.incentive_others, 0)       AS incentive_others,
  COALESCE(i.no_incentive, 0)           AS no_incentive
FROM journeys_agg j
LEFT JOIN incentives_agg i ON j.start_geo_code = i.start_geo_code
  AND j.end_geo_code  = i.end_geo_code
  AND j.journey_date = i.journey_date;

@create_indexes(
  'UNIQUE uq_start_geo_code_end_geo_code_journey_date ON refined_zone.obs_od_day (start_geo_code, end_geo_code, journey_date)',
);