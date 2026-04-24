{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'incremental_date'],
    indexes = [
      { 'columns':['code', 'type', 'incremental_date'], 'unique': true },
    ],
    tags=['refined', 'direction', 'quarter_aom_to']
) }}

WITH filtered_carpools AS (
  {{direction_filtered_carpools_aom(model_column='incremental_date',lookback_nb=1, lookback_unit='quarter')}}
)

SELECT 
  end_aom AS code, 
  'aom' AS type,
  make_date(EXTRACT('year' FROM carpool_datetime)::int, (EXTRACT('quarter' FROM carpool_datetime)::int-1)*3+1, 1) AS incremental_date,
  EXTRACT('year' FROM carpool_datetime)::int AS year,
  EXTRACT('quarter' FROM carpool_datetime)::int AS quarter,
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE is_intra) AS intra_carpools,
  COUNT(*) FILTER (WHERE is_new_driver) AS carpools_new_drivers,
  COUNT(*) FILTER (WHERE is_new_passenger) AS carpools_new_passengers,
  COUNT(DISTINCT operator_trip_id) AS trips,
  COUNT(DISTINCT driver_key) AS unique_drivers,
  COUNT(DISTINCT passenger_key) AS unique_passengers,
  COUNT(DISTINCT driver_key) FILTER (WHERE is_new_driver) AS new_drivers,
  COUNT(DISTINCT passenger_key) FILTER (WHERE is_new_passenger) AS new_passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(distance) AS distance,
  SUM(oi_collectivite) AS incentive_collectivite,
  SUM(oi_operator) AS incentive_operator,
  SUM(oi_other) AS incentive_other,
  COUNT(*) FILTER (WHERE NOT with_incentive) AS no_incentive,
  ARRAY[
    COUNT(*) FILTER (WHERE hour = 0),
    COUNT(*) FILTER (WHERE hour = 1),
    COUNT(*) FILTER (WHERE hour = 2),
    COUNT(*) FILTER (WHERE hour = 3),
    COUNT(*) FILTER (WHERE hour = 4),
    COUNT(*) FILTER (WHERE hour = 5),
    COUNT(*) FILTER (WHERE hour = 6),
    COUNT(*) FILTER (WHERE hour = 7),
    COUNT(*) FILTER (WHERE hour = 8),
    COUNT(*) FILTER (WHERE hour = 9),
    COUNT(*) FILTER (WHERE hour = 10),
    COUNT(*) FILTER (WHERE hour = 11),
    COUNT(*) FILTER (WHERE hour = 12),
    COUNT(*) FILTER (WHERE hour = 13),
    COUNT(*) FILTER (WHERE hour = 14),
    COUNT(*) FILTER (WHERE hour = 15),
    COUNT(*) FILTER (WHERE hour = 16),
    COUNT(*) FILTER (WHERE hour = 17),
    COUNT(*) FILTER (WHERE hour = 18),
    COUNT(*) FILTER (WHERE hour = 19),
    COUNT(*) FILTER (WHERE hour = 20),
    COUNT(*) FILTER (WHERE hour = 21),
    COUNT(*) FILTER (WHERE hour = 22),
    COUNT(*) FILTER (WHERE hour = 23)
  ] AS hours_distribution,
  ARRAY[
    COUNT(*) FILTER (WHERE dist_class = '0-10'),
    COUNT(*) FILTER (WHERE dist_class = '10-20'),
    COUNT(*) FILTER (WHERE dist_class = '20-30'),
    COUNT(*) FILTER (WHERE dist_class = '30-40'),
    COUNT(*) FILTER (WHERE dist_class = '40-50'),
    COUNT(*) FILTER (WHERE dist_class = '>50')
  ] AS dist_distribution
FROM filtered_carpools
WHERE end_aom IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
