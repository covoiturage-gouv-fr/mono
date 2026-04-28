{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'month_com_both']
) }}

SELECT * FROM {{ ref('direction_month_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_both') }}
