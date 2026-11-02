{% macro occupation_exposed(grain) %}

  {% set period = exposed_period(grain) %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year'] + period.cols + ['type', 'code'],
  indexes=[
    {'columns': ['year'] + period.cols + ['type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'occupation'],
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
  {% for model_type, exposed_type in exposed_type_map(type_map) %}
    SELECT
      '{{ exposed_type }}' AS type,
      code,
      year,
      {% for c in period.cols %}{{ c }},{% endfor %}
      carpools             AS journeys,
      trips,
      carpools - no_oi     AS has_incentive,
      carpools,
      passenger_seats
    FROM {{ ref('territory_' ~ grain ~ '_' ~ model_type ~ '_both') }}
    {% if exposed_incremental() %}
      WHERE {{ period.key }} >= (SELECT lookback.{{ period.min }} FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  {% for c in period.cols %}t.{{ c }},{% endfor %}
  t.type,
  t.code,
  p.libelle,
  t.journeys,
  t.trips,
  t.has_incentive,
  round(
    (t.trips + t.passenger_seats)::numeric / nullif(t.trips, 0), 2
  )                                 AS occupation_rate,
  -- centroïde (point) : la carte trace un cercle par zone, pas le contour
  st_asgeojson(p.centroid, 6)::json AS geom
FROM territory AS t
LEFT JOIN {{ perimeters_agg_all() }} AS p
  ON
    t.code = p.code AND t.type = p.type
    AND p.year = least(t.year, (SELECT y FROM max_perim_year))
WHERE t.carpools > 0

{% endmacro %}
