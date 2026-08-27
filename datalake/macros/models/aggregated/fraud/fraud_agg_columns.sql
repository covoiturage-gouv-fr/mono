{% macro fraud_agg_columns(with_incentives=false) %}
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE valid_acquisition_status)     AS carpools_valid,
  COUNT(*) FILTER (WHERE NOT valid_acquisition_status) AS carpools_invalid,
  COUNT(*) FILTER (WHERE fraud_status = 'failed')      AS carpools_fraud,
  COUNT(*) FILTER (WHERE anomaly_status = 'failed')    AS carpools_anomaly,
  COUNT(*) FILTER (WHERE acquisition_status = 'terms_violation_error') AS carpools_terms_violation
  {% if with_incentives %}
  ,
  SUM(campaigns_result_total) FILTER (WHERE fraud_status = 'failed') AS avoided_incentives
  {% endif %}
{% endmacro %}
