{{ config(
  materialized='view',
  tags=['refined', 'directions', 'quarter_com_to']
) }}

SELECT * FROM {{ ref('direction_quarter_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_quarter_plm_to') }}
