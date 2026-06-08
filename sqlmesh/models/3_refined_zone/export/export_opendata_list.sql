MODEL (
  name refined_zone.export_opendata_list,
  kind VIEW,
  tags ['refined', 'export', 'opendata'],
);

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
  FROM trusted_zone.perimeters
  ORDER BY arr, year DESC
),

-- Pre-aggregated semi-join: avoids a correlated EXISTS subplan that re-scans
-- campaigns once per output row (O(n*m)). Planned as a single hash join.
incentivised AS (
  SELECT DISTINCT carpool_v2_id
  FROM trusted_zone.campaigns
  WHERE status = 'validated'
)

SELECT
  j.legacy_id                                                              AS journey_id,
  COALESCE(j.operator_trip_id::text, gen_random_uuid()::text)              AS trip_id,

  -- date filtering column (raw DATE in Europe/Paris)
  (j.start_datetime AT TIME ZONE 'Europe/Paris')::date                     AS start_date_filter,
  -- raw UTC timestamp, exposed so consumers can add a sargable range
  -- predicate that hits the journeys (start_datetime) index
  j.start_datetime                                                         AS start_datetime,

  -- start
  to_char(ts_ceil(j.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD HH24:MI:SS') AS journey_start_datetime,
  to_char(ts_ceil(j.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD') AS journey_start_date,
  to_char(ts_ceil(j.start_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS') AS journey_start_time,
  TRUNC(ST_X(j.start_position)::numeric, gps.precision)                    AS journey_start_lon,
  TRUNC(ST_Y(j.start_position)::numeric, gps.precision)                    AS journey_start_lat,
  gps.arr                                                                  AS journey_start_insee,
  gps.dep                                                                  AS journey_start_department,
  gps.l_arr                                                                AS journey_start_town,
  gps.l_epci                                                               AS journey_start_towngroup,
  gps.l_country                                                            AS journey_start_country,

  -- end
  to_char(ts_ceil(j.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD HH24:MI:SS') AS journey_end_datetime,
  to_char(ts_ceil(j.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD') AS journey_end_date,
  to_char(ts_ceil(j.end_datetime_tz, 600) AT TIME ZONE 'Europe/Paris', 'HH24:MI:SS') AS journey_end_time,
  TRUNC(ST_X(j.end_position)::numeric, gpe.precision)                      AS journey_end_lon,
  TRUNC(ST_Y(j.end_position)::numeric, gpe.precision)                      AS journey_end_lat,
  gpe.arr                                                                  AS journey_end_insee,
  gpe.dep                                                                  AS journey_end_department,
  gpe.l_arr                                                                AS journey_end_town,
  gpe.l_epci                                                               AS journey_end_towngroup,
  gpe.l_country                                                            AS journey_end_country,

  j.passenger_seats,
  j.operator_class,
  j.distance                                                               AS journey_distance,
  ROUND(j.duration / 60.0)                                                 AS journey_duration,

  (inc.carpool_v2_id IS NOT NULL)                                          AS has_incentive,

  -- INSEE occurrence counts (for filtering by API)
  ic.start_insee_count,
  ic.end_insee_count

FROM trusted_zone.journeys j
JOIN trusted_zone.insee_counters ic ON j._id = ic._id
LEFT JOIN incentivised inc ON inc.carpool_v2_id = j._id
LEFT JOIN latest_perimeters gps ON j.start_geo_code = gps.arr
LEFT JOIN latest_perimeters gpe ON j.end_geo_code   = gpe.arr

WHERE j.valid_acquisition_status = TRUE
