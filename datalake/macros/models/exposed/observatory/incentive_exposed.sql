{% macro incentive_exposed(grain) %}

  {% set period = exposed_period(grain) %}
  {# période recalculée depuis la colonne month de territory_month_* #}
  {% set from_month = {
    'month':    'month',
    'quarter':  '(month + 2) / 3',
    'semester': '(month + 5) / 6',
    'year':     none
  }[grain] %}
  {% set key_from_month = period.key | replace(period.cols[0], from_month) if period.cols else period.key %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year'] + period.cols + ['type', 'code'],
  indexes=[
    {'columns': ['year'] + period.cols + ['type', 'code'], 'unique': true}
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
    SELECT max({{ period.key }}) - 1 AS {{ period.min }} FROM {{ this }}
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
      {% for c in period.cols %}{{ from_month }} AS {{ c }},{% endfor %}  -- noqa: RF04
      sum(oi_collectivite) AS collectivite,
      sum(oi_operator)     AS operateur,
      sum(oi_other)        AS autres,
      sum(no_oi)::bigint   AS no_incentive
    FROM {{ ref('territory_month_' ~ model_type ~ '_both') }}
    {% if exposed_incremental() %}
      WHERE {{ key_from_month }} >= (SELECT lookback.{{ period.min }} FROM lookback)
    {% endif %}
    GROUP BY code, year{% for c in period.cols %}, {{ from_month }}{% endfor %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  {% for c in period.cols %}t.{{ c }},{% endfor %}
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

{% endmacro %}
