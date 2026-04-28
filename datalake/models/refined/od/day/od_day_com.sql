{{ config(
  materialized='view',
  tags=['refined', 'od', 'daily', 'day_com']
) }}


SELECT * FROM {{ref('od_day_arr')}}
UNION ALL
SELECT * FROM {{ref('od_day_plm')}}
