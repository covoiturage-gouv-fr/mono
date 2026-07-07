{{ config(severity='warn', tags=['trusted', 'cee']) }}

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*) FROM {{ source('dlk_import', 'cee_cee_applications') }}
  WHERE
    datetime >= {{ window_start(ref('cee'), 'datetime') }}
    AND datetime <= CURRENT_TIMESTAMP
) != (
  SELECT COUNT(*) FROM {{ ref('cee') }}
  WHERE
    datetime >= {{ window_start(ref('cee'), 'datetime') }}
    AND datetime <= CURRENT_TIMESTAMP
)
