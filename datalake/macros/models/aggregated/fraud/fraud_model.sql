{# Comme territory_model par opérateur, sans filtre valid_acquisition_status : tous les
   statuts sont comptés pour mesurer fraudes et anomalies. #}
{% macro fraud_model(perim, grain, direction, with_incentives=false) %}
  {{ perimeter_model(
    'fraud', perim, grain, direction,
    columns=fraud_agg_columns(with_incentives=with_incentives),
    keys=['operator_id', 'operator_name'],
    key_ids=['operator_id'],
    filtered_args={'with_valid': false, 'strict': true},
    on_schema_change='append_new_columns'
  ) }}
{% endmacro %}
