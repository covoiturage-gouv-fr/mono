{{ config(
  materialized='view',
  tags=['refined', 'od', 'daily', 'semester_com']
) }}

SELECT * FROM {{ref('od_semester_arr')}}
UNION ALL
SELECT * FROM {{ref('od_semester_plm')}}
