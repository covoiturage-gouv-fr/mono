{{ config(
  materialized='view',
  tags=['refined', 'od', 'daily', 'month_com']
) }}

SELECT * FROM {{ref('od_month_arr')}}
UNION ALL
SELECT * FROM {{ref('od_month_plm')}}
