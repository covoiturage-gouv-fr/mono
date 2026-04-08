MODEL (
  name refined_zone.obs_journeys_incentives,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  cron '@daily',
  grain (_id),
  tags ['refined', 'observatoire', 'transitory'],
);

-- Classify operator incentive sirets per journey into collectivite/operator/other.
-- Pre-computed to avoid repeating the LATERAL unnest in obs_directions_day and obs_od_day.

WITH operators_siren AS (
  SELECT DISTINCT left(siret, 9) AS siren FROM operator.operators
),
aom_codes AS (
  SELECT DISTINCT code FROM trusted_zone.perimeters_agg WHERE type = 'aom'
),
unnested AS (
  SELECT
    j._id,
    j.start_datetime,
    CASE
      WHEN d.code IS NOT NULL THEN 'collectivite'
      WHEN c.siren IS NOT NULL THEN 'operator'
      WHEN s.siret IS NOT NULL THEN 'other'
    END AS siret_type
  FROM trusted_zone.journeys j
  LEFT JOIN LATERAL (SELECT unnest(j.operator_incentives_sirets) AS siret) s ON TRUE
  LEFT JOIN operators_siren c ON left(s.siret, 9) = c.siren
  LEFT JOIN aom_codes d ON left(s.siret, 9) = d.code
  WHERE j.valid_acquisition_status = true
    AND j.start_datetime >= @start_ts
    AND j.start_datetime < @end_ts
)

SELECT
  _id,
  start_datetime,
  COUNT(*) FILTER (WHERE siret_type = 'collectivite') AS incentive_collectivite,
  COUNT(*) FILTER (WHERE siret_type = 'operator') AS incentive_operator,
  COUNT(*) FILTER (WHERE siret_type = 'other') AS incentive_others,
  COUNT(*) FILTER (WHERE siret_type IS NULL) AS no_incentive
FROM unnested
GROUP BY _id, start_datetime;

@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS obs_journeys_incentives_id_idx ON refined_zone.obs_journeys_incentives (_id));
@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS obs_journeys_incentives_datetime_idx ON refined_zone.obs_journeys_incentives (start_datetime));
