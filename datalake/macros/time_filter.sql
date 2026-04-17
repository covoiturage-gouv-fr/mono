{% macro time_filter(column, model_column=None, type='timestamptz', default_start="(TIMESTAMP '2019-01-01 00:00:00' AT TIME ZONE 'Europe/Paris')", lookback_days=0) %}
 
  {% set cast = '::' ~ type %}
  {% set model_col = model_column if model_column else column %}
  (
    {{ column }} >= (
      {% if var('start', none) %}
        '{{ var("start") }}'{{ cast }}
      {% elif is_incremental() %}
        (
          SELECT COALESCE(
            MAX({{ model_col }}::{{ type }}) - INTERVAL '{{ lookback_days }} days',
            {{ default_start }}
          )
          FROM {{ this }}
        )
      {% else %}
        {{ default_start }}
      {% endif %}
    )
    {% if var('end', none) %}
      AND {{ column }} < '{{ var("end") }}'{{ cast }}
    {% endif %}
  )
{% endmacro %}