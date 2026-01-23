MODEL (
  name refined_zone.obs_directions_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1
  ),
  start '2020-01-01',
  end 'now()',
  grain ['code','type','journey_date','direction'],
  tags ['refined', 'observatoire', 'directions_by_day'],
);

SELECT
  code,
  type,
  journey_date,
  direction,
  SUM(journeys) AS journeys,
  SUM(intra_journeys) AS intra_journeys,
  SUM(drivers) AS drivers,
  SUM(passengers) AS passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(distance) AS distance,
  SUM(incentive_collectivite) AS incentive_collectivite,
  SUM(incentive_operator) AS incentive_operator,
  SUM(incentive_others) AS incentive_others
FROM refined_zone.obs_directions_hours_by_day
WHERE code IS NOT NULL
GROUP BY code, type, journey_date, direction;

CREATE UNIQUE INDEX IF NOT EXISTS obs_directions_by_day_pk ON refined_zone.obs_directions_by_day (code, type, journey_date, direction);
