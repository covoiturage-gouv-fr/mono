{% macro group_by_grain(grain, start_index=3) %}

  {% if grain == 'day' %}
    {{ start_index }}

  {% elif grain in ['month', 'quarter', 'semester'] %}
    {{ start_index }}, {{ start_index + 1 }}, {{ start_index + 2 }}

  {% elif grain == 'year' %}
    {{ start_index }}, {{ start_index + 1 }}

  {% endif %}

{% endmacro %}