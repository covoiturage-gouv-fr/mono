{% macro location_exposed(grain) %}

  {# Projection exposée de la heatmap H3 pré-agrégée : union des types de territoire
     (colonne `type`), lue par api-datalake sur (type, code, période). Remplace la
     vue brute `location` qui scannait `carpools` à la volée. #}

  {% set grain_cols = {
    'month':    ['year', 'month'],
    'quarter':  ['year', 'quarter'],
    'semester': ['year', 'semester'],
    'year':     ['year']
  } %}

  {% if grain not in grain_cols %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
  {% endif %}

  {% set gcols = grain_cols[grain] %}

  {% set type_map = [
    ('com',    'com'),
    ('epci',   'epci'),
    ('aom',    'aom'),
    ('aomreg', 'aom'),
    ('dep',    'dep'),
    ('reg',    'reg'),
    ('country','country')
  ] %}

{{ config(
  materialized='table',
  indexes=[
    {'columns': ['type', 'code'] + grain_cols[grain]}
  ],
  tags=['exposed', 'observatory', 'location']
) }}

{% for model_type, exposed_type in type_map %}
SELECT
  '{{ exposed_type }}'::text AS type,
  territory AS code,
  {% for c in gcols %}{{ c }},
  {% endfor %}hex_z8,
  count
FROM {{ ref('location_' ~ grain ~ '_' ~ model_type) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
