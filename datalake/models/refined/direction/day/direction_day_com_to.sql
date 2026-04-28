{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'day_com_to']
) }}

SELECT * FROM {{ ref('direction_day_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_day_plm_to') }}
