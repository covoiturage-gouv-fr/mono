{{ config(
  materialized='view',
  tags=['refined', 'direction', 'day_com_from']
) }}

SELECT * FROM {{ ref('direction_day_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_day_plm_from') }}
