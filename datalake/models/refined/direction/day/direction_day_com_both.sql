{{ config(
  materialized='view',
  tags=['refined', 'direction', 'day_com_both']
) }}

SELECT 
  code,
  'com' as type,
  incremental_date,
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
FROM {{ ref('direction_day_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_day_plm_both') }}
