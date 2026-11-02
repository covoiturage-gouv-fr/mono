{% macro operators_model(perim, grain, direction) %}
  {{ perimeter_model(
    'operators', perim, grain, direction,
    columns=territory_agg_columns(),
    keys=['operator_id', 'operator_name'],
    key_ids=['operator_id'],
    filtered_args={'strict': true},
    on_schema_change='append_new_columns'
  ) }}
{% endmacro %}
