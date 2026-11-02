{# Fenêtre de recalcul incrémental d'un grain : {'nb': …, 'unit': …}, passée à
   filtered_carpools / time_filter. grains restreint les grains acceptés par le modèle. #}
{% macro grain_lookback(grain, grains=['day', 'month', 'quarter', 'semester', 'year']) %}
  {% set lookbacks = {
    'day':      {'nb': 3, 'unit': 'day'},
    'month':    {'nb': 1, 'unit': 'month'},
    'quarter':  {'nb': 1, 'unit': 'quarter'},
    'semester': {'nb': 1, 'unit': 'semester'},
    'year':     {'nb': 1, 'unit': 'year'}
  } %}
  {% if grain not in grains %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain ~ ". Expected: " ~ grains | join(', ')) }}
  {% endif %}
  {{ return(lookbacks[grain]) }}
{% endmacro %}
