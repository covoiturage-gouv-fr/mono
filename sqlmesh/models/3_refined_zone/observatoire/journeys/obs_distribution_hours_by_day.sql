MODEL (
  name refined_zone.obs_distribution_hours_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date
  ),
  start '2020-01-01',
  end 'now()',
  grain ['start_geo_code','end_geo_code','hour','journey_date'],
  tags ['refined', 'observatoire', 'distribution_hours_by_day'],
);


  SELECT
    _id,
    start_geo_code,
    end_geo_code,
    extract('hour' from  start_datetime_tz)::int as hour,
    start_datetime_tz::date AS journey_date,
    CASE WHEN distance < 10000 then '0-10'
      WHEN (distance >= 10000 AND distance < 20000) THEN '10-20'
      WHEN (distance >= 20000 AND distance < 30000) THEN '20-30'
      WHEN (distance >= 30000 AND distance < 40000) THEN '30-40'
      WHEN (distance >= 40000 AND distance < 50000) THEN '40-50'
    ELSE '>50' END as dist_classe
  FROM trusted_zone.journeys
  WHERE valid_acquisition_status = true
    AND start_datetime_tz BETWEEN @start_ds AND @end_ds
