{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'quarter', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'quarter', 'type', 'code'], 'unique': true}
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
    SELECT max(year * 4 + quarter) - 1 AS min_yq FROM {{ this }}
  ),
{% endif %}

max_perim_year AS (
  SELECT max(year) AS y FROM {{ ref('perimeters_agg') }}
),

territory AS (
  {% for model_type, exposed_type in type_map %}
    SELECT
      '{{ exposed_type }}' AS type,
      code,
      year,
      quarter,
      oi_collectivite      AS collectivite,
      oi_operator          AS operateur,
      oi_other             AS autres,
      no_oi                AS no_incentive
    FROM {{ ref('territory_quarter_' ~ model_type ~ '_both') }}
    {% if is_incremental() %}
      WHERE year * 4 + quarter >= (SELECT lookback.min_yq FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.quarter,
  t.type,
  t.code,
  p.libelle,
  t.collectivite,
  t.operateur,
  t.autres,
  t.no_incentive
FROM territory AS t
LEFT JOIN {{ ref('perimeters_agg') }}
  AS p ON t.code = p.code AND t.type = p.type
AND p.year = least(t.year, (SELECT y FROM max_perim_year))
