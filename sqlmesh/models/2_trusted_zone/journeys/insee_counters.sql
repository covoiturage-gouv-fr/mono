MODEL (
  name trusted_zone.insee_counters,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 31,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@monthly',
  grain '_id',
  tags ['trusted', 'insee_counters'],
);

SELECT
  j._id,
  j.start_datetime,
  j.start_geo_code,
  j.end_geo_code,
  COUNT(*) OVER (
    PARTITION BY date_trunc('month', j.start_datetime AT TIME ZONE 'Europe/Paris'),
                 j.start_geo_code
  ) AS start_insee_count,
  COUNT(*) OVER (
    PARTITION BY date_trunc('month', j.start_datetime AT TIME ZONE 'Europe/Paris'),
                 j.end_geo_code
  ) AS end_insee_count
FROM trusted_zone.journeys j
WHERE j.valid_acquisition_status = TRUE
  AND j.start_datetime >= @start_ts
  AND j.start_datetime < LEAST(
    @end_ts,
    date_trunc('month', @end_ts AT TIME ZONE 'Europe/Paris')
      AT TIME ZONE 'Europe/Paris'
  );

@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS @index_name('insee_counters_id_index') ON trusted_zone.insee_counters (_id));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS @index_name('insee_counters_start_datetime_index') ON trusted_zone.insee_counters (start_datetime));
