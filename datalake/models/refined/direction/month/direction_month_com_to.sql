{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'month_com_to']
) }}

SELECT * FROM{{ ref('direction_month_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_to') }}
