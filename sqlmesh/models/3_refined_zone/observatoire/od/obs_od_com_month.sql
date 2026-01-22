MODEL (
  name refined_zone.obs_od_com_month,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 1
  ),
  start '2020-01-01',
  end 'now()',
  grain ['year', 'month', 'type', 'territory_1', 'territory_2'],
  tags ['refined', 'observatoire', 'od_com'],
);


  SELECT
    start_geo_code as origin,
    end_geo_code   as destination,
    journey_date,
    extract('year' FROM journey_date)::int  AS year,
    extract('month' FROM journey_date)::int AS month,
    sum(journeys)                         AS journeys,
    sum(passenger_seats)                  AS passenger_seats,
    sum(distance)                         AS distance,
    sum(duration)                         AS duration
  FROM trusted_zone.obs_journeys_by_day
  WHERE date_trunc('month', journey_date) >= @start_ts
    AND date_trunc('month', journey_date) < @end_ts
  GROUP BY
    1, 2, 3, 4
