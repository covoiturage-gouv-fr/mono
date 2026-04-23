{{ config(
  materialized='view',
  tags=['refined', 'direction', 'month_com_to']
) }}

SELECT code,
  'com' as type,
  carpool_date,
  carpools,
  intra_carpools,
  carpools_new_drivers,
  carpools_new_passengers,
  trips,
  unique_drivers,
  unique_passengers,
  new_drivers,
  new_passengers,
  passenger_seats,
  distance,
  incentive_collectivite,
  incentive_operator,
  incentive_other,
  no_incentive,
  hours_distribution,
  dist_distribution 
FROM {{ ref('direction_month_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_to') }}
