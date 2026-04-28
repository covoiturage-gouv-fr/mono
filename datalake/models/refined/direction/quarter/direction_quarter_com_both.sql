{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'quarter_com_both']
) }}

SELECT * FROM {{ ref('direction_quarter_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_quarter_plm_both') }}
