{% macro check_user_perim(perim) %}
  {% set perims = ['aom', 'aomreg'] + mb_territory_perims() %}
  {% if perim not in perims %}
    {{ exceptions.raise_compiler_error("Invalid perim: " ~ perim ~ ". Expected: " ~ perims | join(', ')) }}
  {% endif %}
{% endmacro %}
