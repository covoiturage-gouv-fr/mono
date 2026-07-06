{{ config(severity='warn', tags=['trusted', 'incentives']) }}

SELECT pi._id
FROM {{ source('dlk_import', 'policy_incentives') }} AS pi
INNER JOIN {{ ref('incentives') }} AS t ON pi._id = t._id
WHERE
  pi.datetime
  >= {{ window_start(ref('incentives'), 'datetime', lookback_nb=3) }}
  AND pi.datetime <= CURRENT_TIMESTAMP
  AND (
    pi.amount IS DISTINCT FROM t.amount
    OR pi.datetime IS DISTINCT FROM t.datetime
    OR pi.operator_id IS DISTINCT FROM t.operator_id
    OR pi.operator_journey_id IS DISTINCT FROM t.operator_journey_id
  )
