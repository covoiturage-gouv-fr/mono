from sqlmesh import macro

@macro()
def temp_directions_query(evaluator, start_ts, end_ts):
  return f"""
    WITH journeys_raw AS (
      SELECT
        j._id,
        j.driver_id,
        j.passenger_id,
        j.start_geo_code,
        j.end_geo_code,
        j.start_datetime_tz::date        AS journey_date,
        EXTRACT('hour' FROM j.start_datetime_tz) AS hour,
        j.passenger_seats,
        j.distance,
        j.duration,
        j.operator_incentives_sirets,
        CASE
          WHEN j.distance < 10000                        THEN '0-10'
          WHEN j.distance >= 10000 AND j.distance < 20000 THEN '10-20'
          WHEN j.distance >= 20000 AND j.distance < 30000 THEN '20-30'
          WHEN j.distance >= 30000 AND j.distance < 40000 THEN '30-40'
          WHEN j.distance >= 40000 AND j.distance < 50000 THEN '40-50'
          ELSE '>50'
        END AS dist_class
      FROM trusted_zone.journeys j
      WHERE j.valid_acquisition_status = true
        AND j.start_datetime >= '{start_ts}'::date
        AND j.start_datetime <  '{end_ts}'::date
    ),
    unnested_sirets AS (
      SELECT
        a._id,
        s.siret,
        CASE
          WHEN d.code    IS NOT NULL                              THEN 'collectivite'
          WHEN c.siren   IS NOT NULL                              THEN 'operator'
          WHEN s.siret   IS NOT NULL                              THEN 'other'
          ELSE NULL
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
    -- Agrégation par _id pour éviter la fuite des sirets lors des jointures
    -- (on reste grain = journey ici, pas encore de GROUP BY géo)
    journey_years AS (
      SELECT EXTRACT(YEAR FROM '{start_ts}'::date)::int AS year
      UNION
      SELECT EXTRACT(YEAR FROM '{end_ts}'::date)::int
    ),
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
    -- enrichissement des utilisateurs
    obs_users_driver AS (
      SELECT user_id, first_date_driver FROM refined_zone.obs_users
    ),
    obs_users_passenger AS (
      SELECT user_id, first_date_passenger FROM refined_zone.obs_users
    ),
    journeys_enriched AS (
      SELECT
        j._id,
        j.driver_id,
        j.passenger_id,
        j.journey_date,
        j.hour,
        j.dist_class,
        j.passenger_seats,
        j.distance,
        j.duration,
        ps.epci    AS start_epci,
        pe.epci    AS end_epci,
        ps.aom     AS start_aom,
        pe.aom     AS end_aom,
        ps.dep     AS start_dep,
        pe.dep     AS end_dep,
        ps.reg     AS start_reg,
        pe.reg     AS end_reg,
        ps.country AS start_country,
        pe.country AS end_country,
        j.start_geo_code AS start_com,
        j.end_geo_code   AS end_com,
        -- dimensions users
        CASE WHEN ud.first_date_driver    = j.journey_date THEN TRUE ELSE FALSE END AS is_new_driver,
        CASE WHEN up.first_date_passenger = j.journey_date THEN TRUE ELSE FALSE END AS is_new_passenger,
        -- incentives (déjà agrégées par _id)
        COALESCE(i.incentive_collectivite, 0) AS incentive_collectivite,
        COALESCE(i.incentive_operator,     0) AS incentive_operator,
        COALESCE(i.incentive_others,       0) AS incentive_others,
        COALESCE(i.no_incentive,           0) AS no_incentive
      FROM journeys_raw j
      LEFT JOIN perimeters_resolved ps
            ON ps.arr = j.start_geo_code
            AND ps.journey_year = EXTRACT(YEAR FROM j.journey_date)::int
      LEFT JOIN perimeters_resolved pe
            ON pe.arr = j.end_geo_code
            AND pe.journey_year = EXTRACT(YEAR FROM j.journey_date)::int
      LEFT JOIN incentives_agg i   ON i._id = j._id
      LEFT JOIN obs_users_driver   ud ON ud.user_id = j.driver_id
      LEFT JOIN obs_users_passenger up ON up.user_id = j.passenger_id
    ),
    directions_non_aom AS (
      SELECT
        j.*,
        d.direction,
        d.type,
        d.start_code AS code,
        CASE
          WHEN d.intra_start IS NOT NULL
          AND d.intra_start = d.intra_end THEN TRUE ELSE FALSE
        END AS is_intra
      FROM journeys_enriched j
      CROSS JOIN LATERAL (
        VALUES
          ('from','com',     j.start_com,     j.end_com,     j.start_com,     j.end_com),
          ('to',  'com',     j.end_com,       j.start_com,   j.end_com,       j.start_com),
          ('from','epci',    j.start_epci,    j.end_epci,    j.start_epci,    j.end_epci),
          ('to',  'epci',    j.end_epci,      j.start_epci,  j.end_epci,      j.start_epci),
          ('from','dep',     j.start_dep,     j.end_dep,     j.start_dep,     j.end_dep),
          ('to',  'dep',     j.end_dep,       j.start_dep,   j.end_dep,       j.start_dep),
          ('from','reg',     j.start_reg,     j.end_reg,     j.start_reg,     j.end_reg),
          ('to',  'reg',     j.end_reg,       j.start_reg,   j.end_reg,       j.start_reg),
          ('from','country', j.start_country, j.end_country, j.start_country, j.end_country),
          ('to',  'country', j.end_country,   j.start_country,j.end_country,  j.start_country)
      ) AS d(direction, type, start_code, end_code, intra_start, intra_end)
    ),
    directions_aom_classic AS (
      SELECT
        j.*,
        d.direction,
        d.type,
        d.start_code AS code,
        CASE
          WHEN d.intra_start IS NOT NULL
          AND d.intra_start = d.intra_end THEN TRUE ELSE FALSE
        END AS is_intra
      FROM journeys_enriched j
      CROSS JOIN LATERAL (
        VALUES
          ('from','aom', j.start_aom, j.end_aom, j.start_aom, j.end_aom),
          ('to',  'aom', j.end_aom,   j.start_aom, j.end_aom, j.start_aom)
      ) AS d(direction, type, start_code, end_code, intra_start, intra_end)
      WHERE d.start_code NOT IN (SELECT aom FROM trusted_zone.aom_region)
    ),
    directions_aom_regional AS (
      SELECT
        j.*,
        'from'   AS direction,
        'aom'    AS type,
        ar.aom   AS code,
        CASE
          WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom
          THEN TRUE ELSE FALSE
        END AS is_intra
      FROM journeys_enriched j
      INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.start_reg
      WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)

      UNION ALL

      SELECT
        j.*,
        'to'     AS direction,
        'aom'    AS type,
        ar.aom   AS code,
        CASE
          WHEN j.start_reg = j.end_reg AND j.start_aom <> j.end_aom
          THEN TRUE ELSE FALSE
        END AS is_intra
      FROM journeys_enriched j
      INNER JOIN trusted_zone.aom_region ar ON ar.reg = j.end_reg
      WHERE NOT (j.start_aom = j.end_aom AND j.start_aom = ar.aom)
    )

    SELECT
      _id,
      driver_id,
      passenger_id,
      journey_date,
      hour,
      dist_class,
      code,
      type,
      direction,
      is_intra,
      passenger_seats,
      distance,
      duration,
      is_new_driver,
      is_new_passenger,
      incentive_collectivite,
      incentive_operator,
      incentive_others,
      no_incentive
    FROM directions_non_aom  WHERE code IS NOT NULL
    UNION ALL
    SELECT
      _id, driver_id, passenger_id, journey_date, hour, dist_class,
      code, type, direction, is_intra, passenger_seats, distance, duration,
      is_new_driver, is_new_passenger,
      incentive_collectivite, incentive_operator, incentive_others, no_incentive
    FROM directions_aom_classic  WHERE code IS NOT NULL
    UNION ALL
    SELECT
      _id, driver_id, passenger_id, journey_date, hour, dist_class,
      code, type, direction, is_intra, passenger_seats, distance, duration,
      is_new_driver, is_new_passenger,
      incentive_collectivite, incentive_operator, incentive_others, no_incentive
    FROM directions_aom_regional WHERE code IS NOT NULL
  """