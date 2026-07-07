{{ config(severity='warn', tags=['trusted', 'carpools']) }}

SELECT c._id
FROM {{ source('dlk_import', 'carpool_v2_carpools') }} AS c
WHERE
  c.start_datetime
  >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
  AND c.start_datetime <= CURRENT_TIMESTAMP
  AND NOT EXISTS (
    SELECT 1 FROM {{ ref('carpools') }} AS t
    WHERE t._id = c._id
  )
