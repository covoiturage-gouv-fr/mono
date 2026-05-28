{% macro window_start(
  model_ref,
  column,
  type='timestamptz',
  default_start="(TIMESTAMP '2019-01-01 00:00:00' AT TIME ZONE 'Europe/Paris')",
  lookback_nb=0,
  lookback_unit='day'
) %}
  {% if lookback_unit == 'quarter' %}
    {% set interval_expr = lookback_nb * 3 ~ ' months' %}
  {% elif lookback_unit == 'semester' %}
    {% set interval_expr = lookback_nb * 6 ~ ' months' %}
  {% else %}
    {% set interval_expr = lookback_nb ~ ' ' ~ lookback_unit ~ 's' %}
  {% endif %}

  (
    SELECT COALESCE(
      {% if type == 'date' %}
      MAX({{ column }}::{{ type }}),
      {% else %}
      MAX({{ column }}::{{ type }}) - INTERVAL '{{ interval_expr }}',
      {% endif %}
      {{ default_start }}
    )
    FROM {{ model_ref }}
    WHERE {{ column }}::{{ type }} <= CURRENT_TIMESTAMP::{{ type }}
  )
{% endmacro %}
