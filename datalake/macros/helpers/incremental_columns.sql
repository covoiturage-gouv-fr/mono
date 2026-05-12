{% macro incremental_columns(date_col, grain) %}

  {% if grain == 'day' %}

    {{date_col}}::date AS incremental_date

  {% elif grain == 'month' %}

    date_trunc('month', {{date_col}})::date AS incremental_date,
    EXTRACT(year FROM {{date_col}})::int AS year,
    EXTRACT(month FROM {{date_col}})::int AS month

  {% elif grain == 'quarter' %}

    date_trunc('quarter', {{date_col}})::date AS incremental_date,
    EXTRACT(year FROM {{date_col}})::int AS year,
    EXTRACT(quarter FROM {{date_col}})::int AS quarter

  {% elif grain == 'semester' %}

    make_date(
      EXTRACT(year FROM {{date_col}})::int,
      CASE 
        WHEN EXTRACT(month FROM {{date_col}}) <= 6 THEN 1
        ELSE 7
      END,
      1
    ) AS incremental_date,
    EXTRACT(year FROM {{date_col}})::int AS year,
    CASE 
      WHEN EXTRACT(month FROM {{date_col}}) <= 6 THEN 1
      ELSE 2
    END AS semester

  {% elif grain == 'year' %}

    make_date(EXTRACT(year FROM {{date_col}})::int, 1, 1) AS incremental_date,
    EXTRACT(year FROM {{date_col}})::int AS year

  {% else %}

    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}

  {% endif %}

{% endmacro %}