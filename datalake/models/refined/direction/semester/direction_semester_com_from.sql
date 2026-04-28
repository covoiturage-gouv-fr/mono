{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'semester_com_from']
) }}

SELECT * FROM {{ ref('direction_semester_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_semester_plm_from') }}
