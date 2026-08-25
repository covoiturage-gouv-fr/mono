{{ config(
  materialized='view',
  tags=['app_metabase', 'monthly_carpools_not_ok']
) }}

SELECT
  date_trunc('month', start_datetime_tz)::date
    AS incremental_date,
  count(*) FILTER (WHERE acquisition_status = 'failed')
    AS acquisition_failed,
  count(*) FILTER (WHERE acquisition_status = 'canceled')
    AS acquisition_canceled,
  count(*) FILTER (WHERE acquisition_status = 'expired')
    AS acquisition_expired,
  count(*) FILTER (WHERE acquisition_status = 'terms_violation_error')
    AS acquisition_terms_violation_error,
  count(*) FILTER (WHERE fraud_status = 'failed')
    AS fraud_failed,
  count(*) FILTER (WHERE anomaly_status = 'failed')
    AS anomaly_failed
FROM {{ ref('carpools') }}
WHERE NOT valid_acquisition_status
GROUP BY 1
ORDER BY 1
