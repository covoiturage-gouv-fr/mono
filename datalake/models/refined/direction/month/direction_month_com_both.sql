{{ config(
  materialized='view',
  tags=['refined', 'directions', 'month_com_both']
) }}

SELECT * FROM {{ ref('direction_month_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_month_plm_both') }}
