MODEL (
  name refined_zone.obs_directions_users_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  grain ['code','type','journey_date','direction'],
  tags ['refined', 'observatoire', 'directions_users_day'],
);

SELECT
  code,
  type,
  journey_date,
  direction,
  COUNT(DISTINCT driver_id)                                    AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                 AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM refined_zone.obs_directions_base
WHERE code IS NOT NULL
  AND journey_date >= @start_ts::date
  AND journey_date <  @end_ts::date
GROUP BY 1,2,3,4

UNION ALL

SELECT
  code,
  type,
  journey_date,
  'both'                                                       AS direction,
  COUNT(DISTINCT driver_id)                                    AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                 AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM refined_zone.obs_directions_base
WHERE code IS NOT NULL
  AND direction = 'from'  -- une journey = une ligne, pas de doublon
  AND journey_date >= @start_ts::date
  AND journey_date <  @end_ts::date
GROUP BY 1,2,3;

@create_unique_index(@this_model, code, type, journey_date, hour, direction);