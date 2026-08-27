-- Détecte une régression silencieuse dans le plumbing with_incentives : le
-- total des incitations évitées sur les carpools frauduleux doit
-- correspondre entre l'agrégat et trusted.carpools, sur la même fenêtre
-- incrémentale.
{{ config(severity='warn', tags=['aggregated', 'fraud']) }}

{% set fraud_window %}
  {{ window_start(
    ref('fraud_month_country_from'),
    'incremental_date',
    type='timestamp',
    default_start="'2020-01-01 00:00:00'",
    lookback_nb=1,
    lookback_unit='month'
  ) }}
{% endset %}

SELECT 1 AS failure
WHERE (
  SELECT COALESCE(SUM(avoided_incentives), 0)
  FROM {{ ref('fraud_month_country_from') }}
  WHERE incremental_date::timestamp >= {{ fraud_window }}
) != (
  SELECT
    COALESCE(
      SUM(campaigns_result_total) FILTER (WHERE fraud_status = 'failed'), 0
    )
  FROM {{ ref('carpools') }}
  WHERE
    start_datetime_tz >= {{ fraud_window }}
    AND start_datetime_tz <= CURRENT_TIMESTAMP
)
