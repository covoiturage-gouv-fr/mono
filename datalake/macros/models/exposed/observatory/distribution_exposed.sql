{% macro distribution_exposed(grain) %}

  {% set period = exposed_period(grain) %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year'] + period.cols + ['type', 'code', 'direction'],
  indexes=[
    {'columns': ['year'] + period.cols + ['type', 'code', 'direction'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'distribution'],
  pre_hook=(['{{ exposed_types_delete() }}'] if exposed_types() else [])
) }}

{% set type_direction_map = [
  ('com',     'com',     'from'),
  ('com',     'com',     'to'),
  ('com',     'com',     'both'),
  ('epci',    'epci',    'from'),
  ('epci',    'epci',    'to'),
  ('epci',    'epci',    'both'),
  ('aom',     'aom',     'from'),
  ('aom',     'aom',     'to'),
  ('aom',     'aom',     'both'),
  ('aomreg',  'aom',     'from'),
  ('aomreg',  'aom',     'to'),
  ('aomreg',  'aom',     'both'),
  ('dep',     'dep',     'from'),
  ('dep',     'dep',     'to'),
  ('dep',     'dep',     'both'),
  ('reg',     'reg',     'from'),
  ('reg',     'reg',     'to'),
  ('reg',     'reg',     'both'),
  ('country', 'country', 'from'),
  ('country', 'country', 'to'),
  ('country', 'country', 'both'),
  ('custom',  'custom',  'from'),
  ('custom',  'custom',  'to'),
  ('custom',  'custom',  'both'),
] %}

{# dist_distribution : une case par tranche de dist_classes() (17 de 5 km, la 17e = 80+).
   Classe exposée : (libellé, première tranche, dernière tranche), indices 1-based. #}
{% set exposed_classes = [
  ('0-10',  1, 2),
  ('10-20', 3, 4),
  ('20-30', 5, 6),
  ('30-40', 7, 8),
  ('40-50', 9, 10),
  ('>50',   11, 17),
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

distribution AS (
  {% for model_type, exposed_type, direction
    in exposed_type_map(type_direction_map) %}
    SELECT
      '{{ exposed_type }}' AS type,
      '{{ direction }}'    AS direction,
      code,
      year,
      {% for c in period.cols %}{{ c }},{% endfor %}
      hours_distribution,
      dist_distribution
    FROM {{ ref('territory_' ~ grain ~ '_' ~ model_type ~ '_' ~ direction) }}
    {% if exposed_incremental() %}
      WHERE {{ period.key }} >= (SELECT lookback.{{ period.min }} FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  d.year,
  {% for c in period.cols %}d.{{ c }},{% endfor %}
  d.type,
  d.code,
  p.libelle,
  d.direction,
  (
    SELECT
      jsonb_agg(
        jsonb_build_object('hour', gs - 1, 'journeys', d.hours_distribution[gs])
      )
    FROM generate_series(1, 24) AS gs
  ) AS hours,
  jsonb_build_array(
    {% for label, first, last in exposed_classes %}
      jsonb_build_object(
        'dist_classes',
        '{{ label }}',
        'journeys',
        {% for i in range(first, last + 1) %}d.dist_distribution[{{ i }}]{% if not loop.last %} + {% endif %}{% endfor %}
      ){% if not loop.last %},{% endif %}
    {% endfor %}
  ) AS distances
FROM distribution AS d
LEFT JOIN {{ perimeters_agg_all() }} AS p
  ON
    d.code = p.code AND d.type = p.type
    AND p.year = least(d.year, (SELECT y FROM max_perim_year))
WHERE d.code IS NOT NULL

{% endmacro %}
