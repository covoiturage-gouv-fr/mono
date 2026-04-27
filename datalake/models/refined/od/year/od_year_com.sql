{{ config(
  materialized='view',
  tags=['refined', 'od', 'year_com']
) }}

SELECT 
  territory_1,
  territory_2,
  'com' as type,
  incremental_date,
  year,
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
  hours_distribution 
FROM {{ref('od_year_arr')}}
UNION ALL
SELECT * FROM {{ref('od_year_plm')}}
