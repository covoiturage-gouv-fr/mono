{{ config(severity='warn', tags=['trusted', 'cee']) }}

SELECT cee._id
FROM {{ source('cee', 'cee_applications') }} cee
INNER JOIN {{ ref('cee') }} t ON t._id = cee._id
WHERE cee.datetime >= {{ window_start(ref('cee'), 'datetime') }}
  AND cee.datetime <= CURRENT_TIMESTAMP
  AND (
    cee.operator_id            IS DISTINCT FROM t.operator_id
    OR cee.datetime            IS DISTINCT FROM t.datetime
    OR cee.journey_type::VARCHAR IS DISTINCT FROM t.journey_type
    OR cee.operator_journey_id IS DISTINCT FROM t.operator_journey_id
  )
