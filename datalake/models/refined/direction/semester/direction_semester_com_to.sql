{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'semester_com_to']
) }}

SELECT * FROM {{ ref('direction_semester_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_semester_plm_to') }}
