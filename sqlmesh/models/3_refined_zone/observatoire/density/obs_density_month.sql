MODEL (
  name refined_zone.obs_density_month,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column month_date,
    batch_size 3
  ),
  start '2020-01-01',
  cron '@monthly',
  grain [ 'month_date', 'code', 'type', 'direction'],
  tags ['refined', 'observatoire', 'density_month'],
);

SELECT
  date_trunc('month', start_datetime) AS month_date,
  EXTRACT(YEAR FROM start_datetime)::int AS year,
  EXTRACT(MONTH FROM start_datetime)::int AS month,
  h3_index,
  COUNT(*) AS journeys,
  SUM(distance) AS distance,
  SUM(duration) AS duration,
  SUM(passenger_seats) AS passengers
FROM (
  SELECT
    start_datetime,
    start_h3_index AS h3_index,
    distance,
    duration,
    passenger_seats
  FROM trusted_zone.journeys
  WHERE start_datetime >= @start_ds
    AND start_datetime < @end_ds
  UNION ALL
  SELECT
    start_datetime,
    end_h3_index AS h3_index,
    distance,
    duration,
    passenger_seats
  FROM trusted_zone.journeys
  WHERE start_datetime >= @start_ds
    AND start_datetime < @end_ds
) t
GROUP BY 1,2,3;

