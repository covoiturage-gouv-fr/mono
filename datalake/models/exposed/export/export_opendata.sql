{{ config(
    materialized='view',
    tags=['exposed', 'export', 'opendata']
) }}

WITH latest_perimeters AS (
  SELECT DISTINCT ON (arr)
    arr,
    dep,
    country,
    CASE WHEN arr <> country THEN l_arr    ELSE NULL  END AS l_arr,
    l_epci,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    CASE
      WHEN surface > 0 AND (pop::float / (surface / 100.0)) > 40 THEN 3
      ELSE 2
    END AS precision
  FROM {{ ref('perimeters') }}
  ORDER BY arr, year DESC
)

SELECT
  c.legacy_id                                                              AS journey_id,
  COALESCE(c.operator_trip_id::text, gen_random_uuid()::text)              AS trip_id,

  -- date filtering column (raw DATE in Europe/Paris)
  (c.start_datetime AT TIME ZONE 'Europe/Paris')::date                     AS start_date_filter,

  -- start
  to_char(ts_ceil(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD HH24:MI:SS') AS journey_start_datetime,
  to_char(ts_ceil(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD') AS journey_start_date,
  to_char(ts_ceil(c.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS') AS journey_start_time,
  TRUNC(ST_X(sc.start_position::geometry)::numeric, gps.precision)                    AS journey_start_lon,
  TRUNC(ST_Y(sc.start_position::geometry)::numeric, gps.precision)                    AS journey_start_lat,
  gps.arr                                                                  AS journey_start_insee,
  gps.dep                                                                  AS journey_start_department,
  gps.l_arr                                                                AS journey_start_town,
  gps.l_epci                                                               AS journey_start_towngroup,
  gps.l_country                                                            AS journey_start_country,

  -- end
  to_char(ts_ceil(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD HH24:MI:SS') AS journey_end_datetime,
  to_char(ts_ceil(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD') AS journey_end_date,
  to_char(ts_ceil(c.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS') AS journey_end_time,
  TRUNC(ST_X(sc.end_position::geometry)::numeric, gpe.precision)                      AS journey_end_lon,
  TRUNC(ST_Y(sc.end_position::geometry)::numeric, gpe.precision)                      AS journey_end_lat,
  gpe.arr                                                                  AS journey_end_insee,
  gpe.dep                                                                  AS journey_end_department,
  gpe.l_arr                                                                AS journey_end_town,
  gpe.l_epci                                                               AS journey_end_towngroup,
  gpe.l_country                                                            AS journey_end_country,

  c.passenger_seats,
  c.operator_class,
  c.distance                                                               AS journey_distance,
  ROUND(c.duration / 60.0)                                                 AS journey_duration,
  c.with_incentive                                                          AS has_incentive,

  -- INSEE occurrence counts (for filtering by API)
  ts.carpools AS start_insee_count,
  te.carpools AS end_insee_count

FROM {{ref('carpools')}} c
LEFT JOIN {{ source('carpool_v2', 'carpools') }} sc ON sc._id = c._id
LEFT JOIN {{ ref('territory_month_arr_from') }} ts ON c.start_geo_code = ts.code AND date_trunc('month', c.start_datetime) = ts.incremental_date
LEFT JOIN {{ ref('territory_month_arr_to') }} te ON c.end_geo_code = te.code AND date_trunc('month', c.start_datetime) = te.incremental_date
LEFT JOIN latest_perimeters gps ON c.start_geo_code = gps.arr
LEFT JOIN latest_perimeters gpe ON c.end_geo_code   = gpe.arr

WHERE c.valid_acquisition_status = TRUE
