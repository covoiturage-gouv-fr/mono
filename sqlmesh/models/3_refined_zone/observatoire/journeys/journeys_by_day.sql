MODEL (
  name refined_zone.obs_journeys_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1
  ),
  start '2020-01-01',
  end 'now()',
  grain ['start_geo_code','end_geo_code','journey_date'],
  tags ['refined', 'observatoire', 'journeys_by_day'],
);

WITH unnested_sirets AS (
  SELECT
    a._id,
    a.start_geo_code,
    a.end_geo_code,
    a.start_datetime_tz::date AS journey_date,
    s.siret,
    CASE 
      WHEN c.establishment_naf_code IN ('8411Z','8413Z','4931Z','4939A')
        THEN 'collectivite'
      WHEN c.establishment_naf_code IS NOT NULL AND c.establishment_naf_code NOT IN ('8411Z','8413Z','4931Z','4939A')
        THEN 'operator'
      WHEN c.establishment_naf_code IS NULL
        THEN 'other'
      ELSE NULL
    END AS siret_type
  FROM trusted_zone.journeys a
  LEFT JOIN LATERAL (SELECT unnest(a.operator_incentives_sirets) AS siret) s ON TRUE
  LEFT JOIN company.companies c ON s.siret = c.siret
  WHERE a.valid_acquisition_status = true
  AND a.start_datetime_tz BETWEEN @start_ds AND @end_ds
),

incentives_agg AS (
  SELECT
    start_geo_code,
    end_geo_code,
    journey_date,
    COUNT(*) FILTER (WHERE siret_type = 'collectivite') AS incentive_collectivite,
    COUNT(*) FILTER (WHERE siret_type = 'operator')     AS incentive_operator,
    COUNT(*) FILTER (WHERE siret_type = 'other')        AS incentive_others
  FROM unnested_sirets
  GROUP BY 1,2,3
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
AND start_datetime_tz BETWEEN @start_ds AND @end_ds
GROUP BY 1,2,3
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
  COALESCE(i.incentive_others, 0)       AS incentive_others
FROM journeys_agg j
LEFT JOIN incentives_agg i ON j.start_geo_code = i.start_geo_code
  AND j.end_geo_code   = i.end_geo_code
  AND j.journey_date  = i.journey_date;

CREATE UNIQUE INDEX IF NOT EXISTS obs_journeys_by_day_pk ON refined_zone.obs_journeys_by_day (start_geo_code, end_geo_code, journey_date);