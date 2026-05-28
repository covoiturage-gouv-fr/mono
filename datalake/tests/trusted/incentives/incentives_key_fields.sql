{{ config(severity='warn', tags=['trusted', 'incentives']) }}

SELECT pi._id
FROM {{ source('policy', 'incentives') }} pi
INNER JOIN {{ ref('incentives') }} t ON t._id = pi._id
WHERE pi.datetime >= {{ window_start(ref('incentives'), 'datetime', lookback_nb=3) }}
  AND pi.datetime <= CURRENT_TIMESTAMP
  AND (
    pi.amount                 IS DISTINCT FROM t.amount
    OR pi.datetime            IS DISTINCT FROM t.datetime
    OR pi.operator_id         IS DISTINCT FROM t.operator_id
    OR pi.operator_journey_id IS DISTINCT FROM t.operator_journey_id
  )
