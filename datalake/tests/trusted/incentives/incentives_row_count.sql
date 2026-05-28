-- Warn car le modèle peut exclure des incentives sans carpool associé.
{{ config(severity='warn', tags=['trusted', 'incentives']) }}

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*) FROM {{ source('policy', 'incentives') }}
  WHERE datetime >= {{ window_start(ref('incentives'), 'datetime', lookback_nb=3) }}
    AND datetime <= CURRENT_TIMESTAMP
) != (
  SELECT COUNT(*) FROM {{ ref('incentives') }}
  WHERE datetime >= {{ window_start(ref('incentives'), 'datetime', lookback_nb=3) }}
    AND datetime <= CURRENT_TIMESTAMP
)
