MODEL (
  name refined_zone.obs_density_month,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column month_date,
    batch_size 1
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@monthly',
  grain [ 'month_date', 'h3_index'],
  tags ['refined', 'observatoire', 'density_month'],
);

SELECT
  date_trunc('month', start_datetime)    AS month_date,
  EXTRACT(YEAR  FROM start_datetime)::int AS year,
  EXTRACT(MONTH FROM start_datetime)::int AS month,
  h3_index,
  COUNT(*)             AS journeys,
  SUM(distance)        AS distance,
  SUM(duration)        AS duration,
  SUM(passenger_seats) AS passengers
FROM trusted_zone.journeys
CROSS JOIN LATERAL (
  VALUES
    (start_h3_index),
    (end_h3_index)
) AS t(h3_index)
WHERE start_datetime >= @start_ts
  AND start_datetime < @end_ts
GROUP BY 1, 2, 3, 4