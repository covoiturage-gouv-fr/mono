{% macro get_final_acquisition_status_list() %}
    (
      'processed',
      'failed',
      'canceled',
      'expired',
      'terms_violation_error'
    )
{% endmacro %}