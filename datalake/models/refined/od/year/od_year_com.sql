{{ config(
  materialized='view',
  tags=['refined', 'od', 'daily', 'year_com']
) }}

SELECT * FROM  {{ref('od_year_arr')}}
UNION ALL
SELECT * FROM {{ref('od_year_plm')}}
