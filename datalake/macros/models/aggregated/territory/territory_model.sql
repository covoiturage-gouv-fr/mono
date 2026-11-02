{% macro territory_model(perim, grain, direction) %}
  {{ perimeter_model(
    'territory', perim, grain, direction,
    columns=territory_agg_columns(),
    filtered_args={'strict': true},
    on_schema_change='append_new_columns'
  ) }}
{% endmacro %}
