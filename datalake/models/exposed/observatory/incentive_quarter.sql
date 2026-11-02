{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'quarter', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'quarter', 'type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'incentive'],
  pre_hook=(['{{ exposed_types_delete() }}'] if exposed_types() else [])
) }}

{% set type_map = [
  ('com',    'com'),
  ('epci',   'epci'),
  ('aom',    'aom'),
  ('aomreg', 'aom'),
  ('dep',    'dep'),
  ('reg',    'reg'),
  ('country','country'),
  ('custom', 'custom')
] %}

WITH
{% if exposed_incremental() %}
  lookback AS (
    SELECT max(year * 4 + quarter) - 1 AS min_yq FROM {{ this }}
  ),
{% endif %}

max_perim_year AS (
  SELECT max(year) AS y FROM {{ ref('perimeters_agg') }}
),

territory AS (
  {# Compteurs additifs : sommés depuis les mois, les grains longs n'ont pas
     à être recalculés quand une correction ne touche que les mois. #}
  {% for model_type, exposed_type in exposed_type_map(type_map) %}
    SELECT
      '{{ exposed_type }}' AS type,
      code,
      year,
      (month + 2) / 3      AS quarter,  -- noqa: RF04
      sum(oi_collectivite) AS collectivite,
      sum(oi_operator)     AS operateur,
      sum(oi_other)        AS autres,
      sum(no_oi)::bigint   AS no_incentive
    FROM {{ ref('territory_month_' ~ model_type ~ '_both') }}
    {% if exposed_incremental() %}
      WHERE year * 4 + (month + 2) / 3 >= (SELECT lookback.min_yq FROM lookback)
    {% endif %}
    GROUP BY code, year, (month + 2) / 3
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
LEFT JOIN {{ perimeters_agg_all() }}
  AS p ON t.code = p.code AND t.type = p.type
AND p.year = least(t.year, (SELECT y FROM max_perim_year))
