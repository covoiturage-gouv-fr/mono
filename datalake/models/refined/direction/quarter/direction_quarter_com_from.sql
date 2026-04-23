{{ config(
  materialized='view',
  tags=['refined', 'directions', 'quarter_com_from']
) }}

SELECT * FROM {{ ref('direction_quarter_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_quarter_plm_from') }}
