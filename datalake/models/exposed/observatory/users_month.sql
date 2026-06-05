{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'month', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'month', 'type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'users']
) }}

{% set type_map = [
  ('com',    'com'),
  ('epci',   'epci'),
  ('aom',    'aom'),
  ('aomreg', 'aom'),
  ('dep',    'dep'),
  ('reg',    'reg'),
  ('country','country')
] %}

WITH
{% if is_incremental() %}
lookback AS (
  SELECT max(year * 100 + month) - 1 AS min_ym FROM {{ this }}
),
{% endif %}

max_perim_year AS (
  SELECT MAX(year) AS y FROM {{ ref('perimeters_agg') }}
),

territory AS (
  {% for model_type, exposed_type in type_map %}
  SELECT
    '{{ exposed_type }}'  AS type,
    code,
    year,
    month,
    unique_drivers,
    new_drivers,
    unique_passengers,
    new_passengers
  FROM {{ ref('territory_month_' ~ model_type ~ '_both') }}
  {% if is_incremental() %}
  WHERE year * 100 + month >= (SELECT min_ym FROM lookback)
  {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.month,
  t.type,
  t.code,
  p.libelle,
  t.unique_drivers,
  t.new_drivers,
  t.unique_passengers,
  t.new_passengers
FROM territory t
LEFT JOIN {{ ref('perimeters_agg') }} p ON p.code = t.code AND p.type = t.type
  AND p.year = LEAST(t.year, (SELECT y FROM max_perim_year))
