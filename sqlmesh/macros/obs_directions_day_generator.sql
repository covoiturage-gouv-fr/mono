{% macro obs_directions_day_generator(source_table, start_ts, end_ts) %}
/*
  Génère les agrégats quotidiens de directions depuis une table yearly.

  Paramètres :
    source_table : trusted_zone.journeys_YYYY ou trusted_zone.journeys_latest
    start_ts     : borne basse du batch (@start_ts)
    end_ts       : borne haute du batch (@end_ts)

  Deux niveaux d'agrégation pour éviter la matérialisation du grain explosé :
    1. agg_detail → (code, type, journey_date, direction, hour, dist_class)
                    COUNT(DISTINCT) corrects car _id est encore visible
    2. agg_final  → (code, type, journey_date, direction)
                    jsonb_object_agg des distributions

  Résultat : from + to + both
*/

WITH journey_years AS (
  SELECT EXTRACT(YEAR FROM {{start_ts}}::date)::int AS year
  UNION
  SELECT EXTRACT(YEAR FROM {{end_ts}}::date)::int
),

-- Résolution des périmètres pour les années du batch uniquement
perimeters_resolved AS (
  SELECT DISTINCT ON (p.arr, y.year)
    p.arr,
    p.epci,
    p.aom,
    p.dep,
    p.reg,
    p.country,
    y.year AS journey_year
  FROM trusted_zone.perimeters p
  CROSS JOIN journey_years y
  WHERE p.year <= y.year
  ORDER BY p.arr, y.year, p.year DESC
),

journeys_raw AS (
  SELECT
    j._id,
    j.driver_id,
    j.passenger_id,
    j.start_datetime_tz::date                        AS journey_date,
    EXTRACT('hour' FROM j.start_datetime_tz)::int    AS hour,
    j.passenger_seats,
    j.distance,
    j.operator_incentives_sirets,
    CASE
      WHEN j.distance <  10000 THEN '0-10'
      WHEN j.distance <  20000 THEN '10-20'
      WHEN j.distance <  30000 THEN '20-30'
      WHEN j.distance <  40000 THEN '30-40'
      WHEN j.distance <  50000 THEN '40-50'
      ELSE '>50'
    END                                              AS dist_class,
    -- Géographie start
    ps.epci    AS start_epci,
    ps.aom     AS start_aom,
    ps.dep     AS start_dep,
    ps.reg     AS start_reg,
    ps.country AS start_country,
    j.start_geo_code AS start_com,
    -- Géographie end
    pe.epci    AS end_epci,
    pe.aom     AS end_aom,
    pe.dep     AS end_dep,
    pe.reg     AS end_reg,
    pe.country AS end_country,
    j.end_geo_code   AS end_com,
    -- Nouveaux utilisateurs
    (ud.first_date_driver    = j.start_datetime_tz::date) AS is_new_driver,
    (up.first_date_passenger = j.start_datetime_tz::date) AS is_new_passenger
  FROM {{source_table}} j
  -- Index start_datetime utilisé car on lit directement la table yearly
  LEFT JOIN perimeters_resolved ps
         ON ps.arr         = j.start_geo_code
        AND ps.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
  LEFT JOIN perimeters_resolved pe
         ON pe.arr         = j.end_geo_code
        AND pe.journey_year = EXTRACT(YEAR FROM j.start_datetime)::int
  -- obs_users
  LEFT JOIN refined_zone.obs_users ud ON ud.user_id = j.driver_id
  LEFT JOIN refined_zone.obs_users up ON up.user_id = j.passenger_id
  WHERE j.start_datetime >= {{start_ts}}
    AND j.start_datetime <  {{end_ts}}
    AND j.valid_acquisition_status = true
),

-- Qualification des sirets agrégée au grain journey AVANT l'explosion géo
-- → évite toute multiplication des incentives lors du CROSS JOIN
unnested_sirets AS (
  SELECT
    a._id,
    CASE
      WHEN d.code  IS NOT NULL THEN 'collectivite'
      WHEN c.siren IS NOT NULL THEN 'operator'
      WHEN s.siret IS NOT NULL THEN 'other'
    END AS siret_type
  FROM journeys_raw a
  LEFT JOIN LATERAL (SELECT unnest(a.operator_incentives_sirets) AS siret) s ON TRUE
  LEFT JOIN (SELECT DISTINCT left(siret, 9) AS siren FROM operator.operators) c
         ON left(s.siret, 9) = c.siren
  LEFT JOIN (SELECT DISTINCT code FROM trusted_zone.perimeters_agg WHERE type = 'aom') d
         ON left(s.siret, 9) = d.code
),

incentives_agg AS (
  SELECT
    _id,
    COUNT(*) FILTER (WHERE siret_type = 'collectivite') AS incentive_collectivite,
    COUNT(*) FILTER (WHERE siret_type = 'operator')     AS incentive_operator,
    COUNT(*) FILTER (WHERE siret_type = 'other')        AS incentive_others,
    COUNT(*) FILTER (WHERE siret_type IS NULL)          AS no_incentive
  FROM unnested_sirets
  GROUP BY 1
),

journeys_enriched AS (
  SELECT
    j.*,
    COALESCE(i.incentive_collectivite, 0) AS incentive_collectivite,
    COALESCE(i.incentive_operator,     0) AS incentive_operator,
    COALESCE(i.incentive_others,       0) AS incentive_others,
    COALESCE(i.no_incentive,           0) AS no_incentive
  FROM journeys_raw j
  LEFT JOIN incentives_agg i ON i._id = j._id
),

-- Explosion géo — jamais écrite sur disque
-- Le CROSS JOIN produit ~11 lignes par journey mais est immédiatement
-- consommé par agg_detail sans matérialisation intermédiaire
directions_exploded AS (
  -- Dimensions non-AOM (com, epci, dep, reg, country) — from + to
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    d.direction, d.type, d.code,
    (d.code IS NOT NULL AND d.code = d.end_code) AS is_intra
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from','com',     j.start_com,     j.end_com),
      ('to',  'com',     j.end_com,       j.start_com),
      ('from','epci',    j.start_epci,    j.end_epci),
      ('to',  'epci',    j.end_epci,      j.start_epci),
      ('from','dep',     j.start_dep,     j.end_dep),
      ('to',  'dep',     j.end_dep,       j.start_dep),
      ('from','reg',     j.start_reg,     j.end_reg),
      ('to',  'reg',     j.end_reg,       j.start_reg),
      ('from','country', j.start_country, j.end_country),
      ('to',  'country', j.end_country,   j.start_country)
  ) AS d(direction, type, code, end_code)
  WHERE d.code IS NOT NULL

  UNION ALL

  -- AOM classiques (hors AOM régionales)
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    d.direction, 'aom' AS type, d.code,
    (d.code IS NOT NULL AND d.code = d.end_code) AS is_intra
  FROM journeys_enriched j
  CROSS JOIN LATERAL (
    VALUES
      ('from', j.start_aom, j.end_aom),
      ('to',   j.end_aom,   j.start_aom)
  ) AS d(direction, code, end_code)
  WHERE d.code IS NOT NULL
    AND d.code NOT IN (SELECT aom FROM trusted_zone.aom_region)

  UNION ALL

  -- AOM régionales from
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    'from' AS direction, 'aom' AS type, ar.aom AS code,
    (j.start_reg = j.end_reg AND j.start_aom <> j.end_aom) AS is_intra
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)

  UNION ALL

  -- AOM régionales to
  SELECT
    j._id, j.driver_id, j.passenger_id,
    j.journey_date, j.hour, j.dist_class,
    j.passenger_seats, j.distance,
    j.is_new_driver, j.is_new_passenger,
    j.incentive_collectivite, j.incentive_operator,
    j.incentive_others, j.no_incentive,
    'to' AS direction, 'aom' AS type, ar.aom AS code,
    (j.start_reg = j.end_reg AND j.start_aom <> j.end_aom) AS is_intra
  FROM journeys_enriched j
  INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.end_reg
  WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)
),

-- Niveau 1 : grain (code, type, journey_date, direction, hour, dist_class)
-- COUNT(DISTINCT) exacts car _id/driver_id/passenger_id encore visibles
agg_detail AS (
  SELECT
    code,
    type,
    journey_date,
    direction,
    hour,
    dist_class,
    COUNT(DISTINCT _id)                                              AS journeys,
    COUNT(DISTINCT _id)       FILTER (WHERE is_intra)               AS intra_journeys,
    COUNT(DISTINCT driver_id)                                        AS drivers,
    COUNT(DISTINCT passenger_id)                                     AS passengers,
    COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
    COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers,
    SUM(passenger_seats)                                             AS passenger_seats,
    SUM(distance)                                                    AS distance,
    SUM(incentive_collectivite)                                      AS incentive_collectivite,
    SUM(incentive_operator)                                          AS incentive_operator,
    SUM(incentive_others)                                            AS incentive_others,
    SUM(no_incentive)                                                AS no_incentive
  FROM directions_exploded
  GROUP BY 1, 2, 3, 4, 5, 6
),

-- Niveau 2 : grain (code, type, journey_date, direction)
-- jsonb_object_agg sur les buckets déjà agrégés — pas de doublons
agg_final AS (
  SELECT
    code,
    type,
    journey_date,
    direction,
    SUM(journeys)               AS journeys,
    SUM(intra_journeys)         AS intra_journeys,
    SUM(drivers)                AS drivers,
    SUM(passengers)             AS passengers,
    SUM(new_drivers)            AS new_drivers,
    SUM(new_passengers)         AS new_passengers,
    SUM(passenger_seats)        AS passenger_seats,
    SUM(distance)               AS distance,
    SUM(incentive_collectivite) AS incentive_collectivite,
    SUM(incentive_operator)     AS incentive_operator,
    SUM(incentive_others)       AS incentive_others,
    SUM(no_incentive)           AS no_incentive,
    jsonb_object_agg(hour::text, journeys ORDER BY hour)           AS hours_distribution,
    jsonb_object_agg(dist_class, journeys ORDER BY dist_class)     AS dist_distribution
  FROM agg_detail
  GROUP BY 1, 2, 3, 4
)

-- from + to
SELECT
  code, type, journey_date, direction,
  journeys, intra_journeys, drivers, passengers,
  new_drivers, new_passengers, passenger_seats, distance,
  incentive_collectivite, incentive_operator, incentive_others, no_incentive,
  hours_distribution, dist_distribution
FROM agg_final

UNION ALL

-- both : dédupliqué sur direction = 'from'
SELECT
  code, type, journey_date,
  'both'        AS direction,
  journeys, intra_journeys, drivers, passengers,
  new_drivers, new_passengers, passenger_seats, distance,
  incentive_collectivite, incentive_operator, incentive_others, no_incentive,
  hours_distribution, dist_distribution
FROM agg_final
WHERE direction = 'from'

{% endmacro %}