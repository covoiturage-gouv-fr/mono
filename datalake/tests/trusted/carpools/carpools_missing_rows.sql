{{ config(severity='error', tags=['trusted', 'carpools']) }}

SELECT c._id
FROM {{ source('carpool_v2', 'carpools') }} c
WHERE c.start_datetime >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
  AND c.start_datetime <= CURRENT_TIMESTAMP
  AND NOT EXISTS (
    SELECT 1 FROM {{ ref('carpools') }} t
    WHERE t._id = c._id
  )
