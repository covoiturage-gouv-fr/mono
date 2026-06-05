{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'incentive']
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
  SELECT max(year) - 1 AS min_year FROM {{ this }}
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
    oi_collectivite       AS collectivite,
    oi_operator           AS operateur,
    oi_other              AS autres,
    no_oi                 AS no_incentive
  FROM {{ ref('territory_year_' ~ model_type ~ '_both') }}
  {% if is_incremental() %}
  WHERE year >= (SELECT min_year FROM lookback)
  {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.type,
  t.code,
  p.libelle,
  t.collectivite,
  t.operateur,
  t.autres,
  t.no_incentive
FROM territory t
LEFT JOIN {{ ref('perimeters_agg') }} p ON p.code = t.code AND p.type = t.type
  AND p.year = LEAST(t.year, (SELECT y FROM max_perim_year))
