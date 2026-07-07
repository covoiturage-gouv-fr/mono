-- COUNT(DISTINCT _id) côté modèle car unique_key inclut les champs de statut.
{{ config(severity='warn', tags=['trusted', 'carpools']) }}

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*) FROM {{ source('dlk_import', 'carpool_v2_carpools') }}
  WHERE
    start_datetime
    >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
    AND start_datetime <= CURRENT_TIMESTAMP
) != (
  SELECT COUNT(DISTINCT _id) FROM {{ ref('carpools') }}
  WHERE
    start_datetime
    >= {{ window_start(ref('carpools'), 'start_datetime', lookback_nb=3) }}
    AND start_datetime <= CURRENT_TIMESTAMP
)
