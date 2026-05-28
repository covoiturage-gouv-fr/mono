{% macro time_filter(
  column, 
  model_column=None, 
  type='timestamptz', 
  default_start="(TIMESTAMP '2019-01-01 00:00:00' AT TIME ZONE 'Europe/Paris')", 
  lookback_nb=0, 
  lookback_unit='day'
) %}
  {% set cast = '::' ~ type %}
  {% set model_col = model_column if model_column else column %}

  {% if lookback_unit == 'quarter' %}
    {% set interval_expr = lookback_nb * 3 ~ ' months' %}
  {% elif lookback_unit == 'semester' %}
    {% set interval_expr = lookback_nb * 6 ~ ' months' %}
  {% else %}
    {% set interval_expr = lookback_nb ~ ' ' ~ lookback_unit ~ 's' %}
  {% endif %}

  (
    {{ column }}{{ cast }} >= (
      {% if var('start', none) %}
        '{{ var("start") }}'{{ cast }}
      {% elif is_incremental() %}
        {{ window_start(this, model_col, type, default_start, lookback_nb, lookback_unit) }}
      {% else %}
        {{ default_start }}
      {% endif %}
    )
    AND {{ column }}{{ cast }} <= CURRENT_TIMESTAMP{{ cast }}
    {% if var('end', none) %}
      AND {{ column }}{{ cast }} < '{{ var("end") }}'{{ cast }}
    {% endif %}
  )
{% endmacro %}