{{ config(
  materialized='view',
  tags=['refined', 'od', 'daily', 'quarter_com']
) }}

SELECT * FROM {{ref('od_quarter_arr')}}
UNION ALL
SELECT * FROM {{ref('od_quarter_plm')}}
