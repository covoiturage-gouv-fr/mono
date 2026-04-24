{{ config(
  materialized='view',
  tags=['refined', 'direction', 'year_com_to']
) }}

SELECT 
  code,
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
  hours_distribution,
  dist_distribution 
FROM {{ ref('direction_year_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_year_plm_to') }}
