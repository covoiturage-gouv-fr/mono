-- sqlmesh/models/3_opendata_zone/datagouv_journeys.sql
MODEL (
  name opendata_zone.datagouv_journeys,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_start_datetime
  ),
  dialect postgres,
  tags ['opendata', 'journeys', 'export'],
  grain journey_id
);

WITH

-- Base : trusted_zone.journeys, filtrée sur la fenêtre temporelle et le statut d'acquisition
base AS (
  SELECT *
  FROM trusted_zone.journeys j
  WHERE j.start_datetime BETWEEN @start_ts AND @end_ts
    AND j.acquisition_status = 'processed'  -- même filtre que dans le TS
),

-- Codes INSEE de départ avec < min_occurrences trajets
exclude_start_full AS (
  SELECT
    start_geo_code,
    array_agg(_id) AS ids
  FROM base
  WHERE start_geo_code IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) < 6
),

-- Codes INSEE d'arrivée avec < min_occurrences trajets
exclude_end_full AS (
  SELECT
    end_geo_code,
    array_agg(_id) AS ids
  FROM base
  WHERE end_geo_code IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) < 6
),

-- Consolidation des _id à exclure
exclude_id AS (
  SELECT *
  FROM (
    SELECT UNNEST(ids) AS _id FROM exclude_start_full
    UNION ALL
    SELECT UNNEST(ids) AS _id FROM exclude_end_full
  ) AS e
  GROUP BY 1
),

-- Sélection des trajets admissibles (comme CTE trips dans le TS)
trips AS (
  SELECT
    j.legacy_id AS journey_id,
    j.operator_trip_id AS trip_id,
    j.operator_class,
    j.start_datetime,
    j.end_datetime,
    j.distance,
    ST_Y(j.start_position::geometry) AS start_lat,
    ST_X(j.start_position::geometry) AS start_lon,
    ST_Y(j.end_position::geometry)   AS end_lat,
    ST_X(j.end_position::geometry)   AS end_lon,
    j.start_geo_code,
    j.end_geo_code,
    j.passenger_seats,
    j.operator_id,
    j.operator_journey_id
  FROM base j
  WHERE j._id NOT IN (SELECT _id FROM exclude_id)
),

-- mêmes règles que "incentives" dans ton TS : prendre une seule incitation par (operator_id, operator_journey_id)
incentives AS (
  SELECT
    pi.operator_id,
    pi.operator_journey_id,
    ROW_NUMBER() OVER (
      PARTITION BY pi.operator_id, pi.operator_journey_id
      ORDER BY pi.amount DESC
    ) AS idx
  FROM policy.incentives pi
  JOIN trips t
    ON t.operator_id = pi.operator_id
   AND t.operator_journey_id = pi.operator_journey_id
  WHERE pi.status = 'validated'
),

-- Perimètres géographiques pour start / end (copié de ton TS)
geo AS (
  SELECT DISTINCT ON (arr)
    arr,
    CASE WHEN arr <> country THEN l_arr ELSE NULL END AS l_arr,
    l_com,
    l_epci,
    dep,
    l_reg,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    l_aom,
    CASE
      WHEN surface > 0::double precision
       AND (pop::double precision / (surface::double precision / 100::double precision)) > 40::double precision
      THEN 3
      ELSE 2
    END AS precision
  FROM geo.perimeters
  WHERE arr IN (
    SELECT UNNEST(ARRAY[start_geo_code, end_geo_code]) FROM trips
  )
  ORDER BY arr, year DESC
)

-- Résultat final : colonnes identiques à DataGouvListType
SELECT
  trips.journey_id,

  COALESCE(trips.trip_id::text, uuid_generate_v4()::text) AS trip_id,

  -- start
  ts_ceil(trips.start_datetime AT TIME ZONE 'Europe/Paris', 600) AS journey_start_datetime,
  to_char(ts_ceil(trips.start_datetime AT TIME ZONE 'Europe/Paris', 600), 'YYYY-MM-DD') AS journey_start_date,
  to_char(ts_ceil(trips.start_datetime AT TIME ZONE 'Europe/Paris', 600), 'HH24:MI:SS') AS journey_start_time,
  trunc(trips.start_lon::numeric, gps.precision) AS journey_start_lon,
  trunc(trips.start_lat::numeric, gps.precision) AS journey_start_lat,
  gps.arr         AS journey_start_insee,
  gps.dep         AS journey_start_department,
  gps.l_arr       AS journey_start_town,
  gps.l_epci      AS journey_start_towngroup,
  gps.l_country   AS journey_start_country,

  -- end
  ts_ceil(trips.end_datetime AT TIME ZONE 'Europe/Paris', 600) AS journey_end_datetime,
  to_char(ts_ceil(trips.end_datetime AT TIME ZONE 'Europe/Paris', 600), 'YYYY-MM-DD') AS journey_end_date,
  to_char(ts_ceil(trips.end_datetime AT TIME ZONE 'Europe/Paris', 600), 'HH24:MI:SS') AS journey_end_time,
  trunc(trips.end_lon::numeric, gpe.precision) AS journey_end_lon,
  trunc(trips.end_lat::numeric, gpe.precision) AS journey_end_lat,
  gpe.arr         AS journey_end_insee,
  gpe.dep         AS journey_end_department,
  gpe.l_arr       AS journey_end_town,
  gpe.l_epci      AS journey_end_towngroup,
  gpe.l_country   AS journey_end_country,

  trips.passenger_seats,
  trips.operator_class,
  trips.distance AS journey_distance,
  ROUND(EXTRACT(EPOCH FROM trips.end_datetime - trips.start_datetime) / 60) AS journey_duration,

  (i.operator_id IS NOT NULL) AS has_incentive,

  -- utile pour filtrer par mois dans le script d'export
  date_trunc('month', trips.start_datetime AT TIME ZONE 'Europe/Paris')::date AS journey_month

FROM trips
LEFT JOIN geo AS gps
  ON trips.start_geo_code = gps.arr
LEFT JOIN geo AS gpe
  ON trips.end_geo_code = gpe.arr
LEFT JOIN incentives i
  ON trips.operator_id = i.operator_id
 AND trips.operator_journey_id = i.operator_journey_id
 AND i.idx = 1
ORDER BY trips.start_datetime ASC;
