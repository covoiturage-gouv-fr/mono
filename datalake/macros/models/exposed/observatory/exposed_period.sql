{# Période d'un modèle exposé : colonne qui suit year (aucune pour l'année), clé de tri
   year * n + période pour le lookback incrémental, et alias de son minimum. #}
{% macro exposed_period(grain) %}
  {% set periods = {
    'month':    {'cols': ['month'],    'key': 'year * 12 + month',  'min': 'min_ym'},
    'quarter':  {'cols': ['quarter'],  'key': 'year * 4 + quarter', 'min': 'min_yq'},
    'semester': {'cols': ['semester'], 'key': 'year * 2 + semester', 'min': 'min_ys'},
    'year':     {'cols': [],           'key': 'year',               'min': 'min_year'}
  } %}
  {% if grain not in periods %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain ~ ". Expected: month, quarter, semester, year") }}
  {% endif %}
  {{ return(periods[grain]) }}
{% endmacro %}
