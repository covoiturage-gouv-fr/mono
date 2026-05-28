{{ config(severity='warn', tags=['trusted', 'carpools']) }}

SELECT c._id
FROM {{ source('carpool_v2', 'carpools') }} c
INNER JOIN {{ ref('carpools') }} t ON t._id = c._id
WHERE c.start_datetime >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
  AND c.start_datetime <= CURRENT_TIMESTAMP
  AND (
    c.operator_id               IS DISTINCT FROM t.operator_id
    OR c.start_datetime         IS DISTINCT FROM t.start_datetime
    OR c.distance               IS DISTINCT FROM t.distance
    OR c.driver_revenue         IS DISTINCT FROM t.driver_revenue
    OR c.passenger_contribution IS DISTINCT FROM t.passenger_contribution
  )
