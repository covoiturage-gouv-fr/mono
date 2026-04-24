{{ config(
  materialized='view',
  tags=['refined', 'direction', 'month_com_from']
) }}

SELECT code,
  'com' as type,
  incremental_date,
  year,
  month,
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
FROM {{ ref('direction_month_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_from') }}
