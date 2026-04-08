MODEL (
  name trusted_zone.cee_primo_drivers,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (driver_hash)
  ),
  start '2019-09-24 00:00:00+0200',
  end '2025-05-25 00:00:00+0200',
  grain (driver_hash),
  tags ['trusted', 'cee'],
);

WITH batch AS (
  SELECT
    driver_id,
    MIN(start_datetime) AS first_datetime
  FROM trusted_zone.journeys
  WHERE valid_acquisition_status = true
    AND driver_id IS NOT NULL
    AND start_datetime BETWEEN @start_ts AND @end_ts
  GROUP BY driver_id
),

batch_with_id AS (
  SELECT DISTINCT ON (md5(b.driver_id))
    md5(b.driver_id) AS driver_hash,
    j._id AS carpool_v2_id,
    b.first_datetime
  FROM batch b
  INNER JOIN trusted_zone.journeys j
    ON j.driver_id = b.driver_id
    AND j.start_datetime = b.first_datetime
    AND j.valid_acquisition_status = true
  ORDER BY md5(b.driver_id), j._id
)

SELECT
  b.driver_hash::varchar AS driver_hash,
  (CASE
    WHEN t.first_datetime IS NULL OR b.first_datetime <= t.first_datetime
    THEN b.carpool_v2_id
    ELSE t.carpool_v2_id
  END)::integer AS carpool_v2_id,
  LEAST(
    COALESCE(t.first_datetime, b.first_datetime),
    b.first_datetime
  )::timestamp AS first_datetime
FROM batch_with_id b
LEFT JOIN trusted_zone.cee_primo_drivers t ON t.driver_hash = b.driver_hash;

@IF(@runtime_stage = 'creating', CREATE UNIQUE INDEX IF NOT EXISTS cee_primo_drivers_pk ON trusted_zone.cee_primo_drivers (driver_hash));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS cee_primo_drivers_carpool_v2_id ON trusted_zone.cee_primo_drivers (carpool_v2_id));
