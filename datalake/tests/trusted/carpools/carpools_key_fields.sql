{{ config(severity='warn', tags=['trusted', 'carpools']) }}

SELECT c._id
FROM {{ source('dlk_import', 'carpool_v2_carpools') }} AS c
INNER JOIN {{ ref('carpools') }} AS t ON c._id = t._id
WHERE
  c.start_datetime
  >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
  AND c.start_datetime <= CURRENT_TIMESTAMP
  AND (
    c.operator_id IS DISTINCT FROM t.operator_id
    OR c.start_datetime IS DISTINCT FROM t.start_datetime
    OR c.distance IS DISTINCT FROM t.distance
    OR c.driver_revenue IS DISTINCT FROM t.driver_revenue
    OR c.passenger_contribution IS DISTINCT FROM t.passenger_contribution
  )
