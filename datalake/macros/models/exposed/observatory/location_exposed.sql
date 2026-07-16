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

  {# Clé de tri de période, pour un lookback incrémental (comme od_month). #}
  {% set period_key = {
    'month':    'year * 100 + month',
    'quarter':  'year * 10 + quarter',
    'semester': 'year * 10 + semester',
    'year':     'year'
  }[grain] %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['type', 'code'] + grain_cols[grain] + ['hex_z8'],
  indexes=[
    {'columns': ['type', 'code'] + grain_cols[grain]}
  ],
  tags=['exposed', 'observatory', 'location']
) }}

{% if is_incremental() %}
WITH lookback AS (
  SELECT max({{ period_key }}) - 1 AS min_key FROM {{ this }}
)
{% endif %}

{% for model_type, exposed_type in type_map %}
SELECT
  '{{ exposed_type }}'::text AS type,
  territory AS code,
  {% for c in gcols %}{{ c }},
  {% endfor %}hex_z8,
  count
FROM {{ ref('location_' ~ grain ~ '_' ~ model_type) }}
{% if is_incremental() %}WHERE {{ period_key }} >= (SELECT min_key FROM lookback){% endif %}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
