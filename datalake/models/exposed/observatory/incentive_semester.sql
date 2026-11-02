{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'semester', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'semester', 'type', 'code'], 'unique': true}
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
    SELECT max(year * 2 + semester) - 1 AS min_ys FROM {{ this }}
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
      (month + 5) / 6      AS semester,
      sum(oi_collectivite) AS collectivite,
      sum(oi_operator)     AS operateur,
      sum(oi_other)        AS autres,
      sum(no_oi)::bigint   AS no_incentive
    FROM {{ ref('territory_month_' ~ model_type ~ '_both') }}
    {% if exposed_incremental() %}
      WHERE year * 2 + (month + 5) / 6 >= (SELECT lookback.min_ys FROM lookback)
    {% endif %}
    GROUP BY code, year, (month + 5) / 6
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.semester,
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
