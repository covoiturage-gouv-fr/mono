{{ config(
    materialized='view',
    tags=['exposed', 'export', 'opendata']
) }}

WITH latest_perimeters AS (
  SELECT DISTINCT ON (arr)
    arr,
    dep,
    country,
    l_epci,
    CASE WHEN arr <> country THEN l_arr END                AS l_arr,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    CASE
      WHEN surface > 0 AND (pop::float / (surface / 100.0)) > 40 THEN 3
      ELSE 2
    END                                                    AS precision
  FROM {{ ref('perimeters') }}
  ORDER BY arr ASC, year DESC
)

SELECT
  c.legacy_id
    AS journey_id,
  gps.arr
    AS journey_start_insee,

  -- date filtering column (raw DATE in Europe/Paris)
  gps.dep
    AS journey_start_department,

  -- start
  gps.l_arr
    AS journey_start_town,
  gps.l_epci
    AS journey_start_towngroup,
  gps.l_country
    AS journey_start_country,
  gpe.arr
    AS journey_end_insee,
  gpe.dep
    AS journey_end_department,
  gpe.l_arr
    AS journey_end_town,
  gpe.l_epci
    AS journey_end_towngroup,
  gpe.l_country
    AS journey_end_country,
  c.passenger_seats,
  c.operator_class,

  -- end
  c.distance
    AS journey_distance,
  c.with_incentive
    AS has_incentive,
  ts.carpools
    AS start_insee_count,
  te.carpools
    AS end_insee_count,
  COALESCE(
    c.operator_trip_id::text, GEN_RANDOM_UUID()::text
  )                AS trip_id,
  (
    c.start_datetime AT TIME ZONE 'Europe/Paris'
  )::date          AS start_date_filter,
  TO_CHAR(
    TS_CEIL(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris',
    'YYYY-MM-DD HH24:MI:SS'
  )                AS journey_start_datetime,
  TO_CHAR(
    TS_CEIL(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD'
  )                AS journey_start_date,
  TO_CHAR(
    TS_CEIL(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS'
  )                AS journey_start_time,
  TRUNC(
    ST_X(sc.start_position::geometry)::numeric, gps.precision
  )                AS journey_start_lon,

  TRUNC(
    ST_Y(sc.start_position::geometry)::numeric, gps.precision
  )                AS journey_start_lat,
  TO_CHAR(
    TS_CEIL(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris',
    'YYYY-MM-DD HH24:MI:SS'
  )                AS journey_end_datetime,
  TO_CHAR(
    TS_CEIL(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD'
  )                AS journey_end_date,
  TO_CHAR(
    TS_CEIL(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS'
  )                AS journey_end_time,
  TRUNC(
    ST_X(sc.end_position::geometry)::numeric, gpe.precision
  )                AS journey_end_lon,

  -- INSEE occurrence counts (for filtering by API)
  TRUNC(
    ST_Y(sc.end_position::geometry)::numeric, gpe.precision
  )                AS journey_end_lat,
  ROUND(
    c.duration / 60.0
  )                AS journey_duration

FROM {{ ref('carpools') }} AS c
LEFT JOIN {{ source('dlk_import', 'carpool_v2_carpools') }} AS sc ON c._id = sc._id
LEFT JOIN
  {{ ref('territory_month_arr_from') }} AS ts
  ON
    c.start_geo_code = ts.code
    AND DATE_TRUNC('month', c.start_datetime) = ts.incremental_date
LEFT JOIN
  {{ ref('territory_month_arr_to') }} AS te
  ON
    c.end_geo_code = te.code
    AND DATE_TRUNC('month', c.start_datetime) = te.incremental_date
LEFT JOIN latest_perimeters AS gps ON c.start_geo_code = gps.arr
LEFT JOIN latest_perimeters AS gpe ON c.end_geo_code = gpe.arr

WHERE c.valid_acquisition_status = TRUE
