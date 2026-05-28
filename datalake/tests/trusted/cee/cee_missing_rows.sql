{{ config(severity='error', tags=['trusted', 'cee']) }}

SELECT cee._id
FROM {{ source('cee', 'cee_applications') }} cee
WHERE cee.datetime >= {{ window_start(ref('cee'), 'datetime') }}
  AND cee.datetime <= CURRENT_TIMESTAMP
  AND NOT EXISTS (
    SELECT 1 FROM {{ ref('cee') }} t
    WHERE t._id = cee._id
  )
