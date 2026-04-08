MODEL (
  name refined_zone.obs_users,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (user_id)
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain 'user_id',
  tags ['refined', 'observatoire', 'users'],
);

WITH journeys AS (
  SELECT
      driver_id AS user_id,
      start_datetime   AS driver_date,
      NULL::timestamp    AS passenger_date,
      start_datetime   AS journey_date,
      1::bigint           AS driver_count,
      0::bigint           AS passenger_count
  FROM trusted_zone.journeys
  WHERE start_datetime BETWEEN @start_ts AND @end_ts
  AND driver_id IS NOT NULL
  UNION ALL
  SELECT
      passenger_id AS user_id,
      NULL::timestamp       AS driver_date,
      start_datetime     AS passenger_date,
      start_datetime   AS journey_date,
      0::bigint           AS driver_count,
      1::bigint           AS passenger_count
  FROM trusted_zone.journeys
  WHERE start_datetime BETWEEN @start_ts AND @end_ts
  AND passenger_id IS NOT NULL
),
agg AS (
  SELECT
    user_id,
    MIN(driver_date)     AS first_date_driver,
    MIN(passenger_date) AS first_date_passenger,
    MAX(driver_date)  AS last_date_driver,
    MAX(passenger_date) AS last_date_passenger,
    SUM(driver_count)    AS driver_count,
    SUM(passenger_count) AS passenger_count,
    SUM(driver_count + passenger_count) AS total_count
  FROM journeys
  GROUP BY user_id
)

SELECT
  a.user_id::varchar,
  NULLIF(
    LEAST(
      COALESCE(t.first_date_driver, TIMESTAMP '9999-12-31'),
      COALESCE(a.first_date_driver, TIMESTAMP '9999-12-31')
    ),
    TIMESTAMP '9999-12-31'
  )::date AS first_date_driver,
  NULLIF(
    LEAST(
      COALESCE(t.first_date_passenger, TIMESTAMP '9999-12-31'),
      COALESCE(a.first_date_passenger, TIMESTAMP '9999-12-31')
    ),
    TIMESTAMP '9999-12-31'
  )::date AS first_date_passenger,
  GREATEST(
    COALESCE(t.last_date_driver, TIMESTAMP '1970-01-01'),
    COALESCE(a.last_date_driver, TIMESTAMP '1970-01-01')
  )::date AS last_date_driver,
  GREATEST(
    COALESCE(t.last_date_passenger, TIMESTAMP '1970-01-01'),
    COALESCE(a.last_date_passenger, TIMESTAMP '1970-01-01')
  )::date AS last_date_passenger,
  (COALESCE(t.driver_count, 0) + COALESCE(a.driver_count, 0))::integer
    AS driver_count,
  (COALESCE(t.passenger_count, 0) + COALESCE(a.passenger_count, 0))::integer
    AS passenger_count,
  (COALESCE(t.total_count, 0) + COALESCE(a.total_count, 0))::integer
    AS total_count
FROM agg a
LEFT JOIN refined_zone.obs_users t ON t.user_id = a.user_id;

@IF(@runtime_stage = 'creating', CREATE UNIQUE INDEX IF NOT EXISTS obs_users_pk ON refined_zone.obs_users (user_id));
