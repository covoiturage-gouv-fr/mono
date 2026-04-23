{{ config(
  materialized='view',
  tags=['refined', 'direction', 'day_com_both']
) }}

SELECT * FROM {{ ref('direction_day_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_day_plm_both') }}
