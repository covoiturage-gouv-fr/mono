{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'month_com_from']
) }}

SELECT * FROM {{ ref('direction_month_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_from') }}
