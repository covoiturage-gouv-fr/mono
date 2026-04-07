MODEL (
  name refined_zone.obs_journeys_geo,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain (_id),
  tags ['refined', 'observatoire', 'transitory'],
);

-- Pre-resolve geographic dimensions (epci, aom, dep, reg, country) per journey.
-- Avoids repeated perimeters DISTINCT ON lookups in obs_directions_day and obs_directions_users_*.

WITH journey_years AS (
  SELECT EXTRACT(YEAR FROM @start_ts::date)::int AS year
  UNION
  SELECT EXTRACT(YEAR FROM @end_ts::date)::int
),
perimeters_resolved AS (
  SELECT DISTINCT ON (p.arr, y.year)
    p.arr, p.epci, p.aom, p.dep, p.reg, p.country,
    y.year AS journey_year
  FROM trusted_zone.perimeters p
  CROSS JOIN journey_years y
  WHERE p.year <= y.year
  ORDER BY p.arr, y.year, p.year DESC
)

SELECT
  j._id,
  j.start_datetime,
  j.start_geo_code,
  j.end_geo_code,
  ps.epci AS start_epci,
  ps.aom AS start_aom,
  ps.dep AS start_dep,
  ps.reg AS start_reg,
  ps.country AS start_country,
  pe.epci AS end_epci,
  pe.aom AS end_aom,
  pe.dep AS end_dep,
  pe.reg AS end_reg,
  pe.country AS end_country
FROM trusted_zone.journeys j
LEFT JOIN perimeters_resolved ps
  ON ps.arr = j.start_geo_code
  AND ps.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
LEFT JOIN perimeters_resolved pe
  ON pe.arr = j.end_geo_code
  AND pe.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
WHERE j.valid_acquisition_status = true
  AND j.start_datetime >= @start_ts
  AND j.start_datetime < @end_ts;

@create_indexes(
  'obs_journeys_geo_id_idx ON refined_zone.obs_journeys_geo (_id)',
  'obs_journeys_geo_datetime_idx ON refined_zone.obs_journeys_geo (start_datetime)',
);
