-- Warn car le modèle exclut via JOIN les incentives sans carpool associé.
{{ config(severity='warn', tags=['trusted', 'incentives']) }}

SELECT pi._id
FROM {{ source('dlk_import', 'policy_incentives') }} AS pi
WHERE
  pi.datetime
  >= {{ window_start(ref('incentives'), 'datetime', lookback_nb=3) }}
  AND pi.datetime <= CURRENT_TIMESTAMP
  AND NOT EXISTS (
    SELECT 1 FROM {{ ref('incentives') }} AS t
    WHERE t._id = pi._id
  )
