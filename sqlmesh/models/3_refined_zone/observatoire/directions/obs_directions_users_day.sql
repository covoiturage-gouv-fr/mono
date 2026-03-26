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

@create_temp_table('refined_zone.temp_directions_users', @temp_directions_query(@start_ts, @end_ts));
@create_unique_index('refined_zone.temp_directions_users', _id, code, type, direction);

SELECT
  code,
  type,
  journey_date,
  direction,
  COUNT(DISTINCT driver_id)                                    AS unique_drivers,
  COUNT(DISTINCT passenger_id)                                 AS unique_passengers,
  COUNT(DISTINCT CASE WHEN is_new_driver    THEN driver_id    END) AS new_drivers,
  COUNT(DISTINCT CASE WHEN is_new_passenger THEN passenger_id END) AS new_passengers
FROM refined_zone.temp_directions_users
WHERE code IS NOT NULL
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
FROM refined_zone.temp_directions_users
WHERE code IS NOT NULL
  AND direction = 'from'  -- une journey = une ligne, pas de doublon
GROUP BY 1,2,3;

@drop_temp_table('refined_zone.temp_directions_users');
@create_unique_index(@this_model, code, type, journey_date, hour, direction);