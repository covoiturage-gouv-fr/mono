{% macro od_exposed(grain) %}

  {% set period = exposed_period(grain) %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year'] + period.cols + ['type', 'territory_1', 'territory_2'],
  indexes=[
    {'columns': ['year'] + period.cols + ['type', 'territory_1', 'territory_2'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'od'],
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

od AS (
  {% for model_type, exposed_type in exposed_type_map(type_map) %}
    SELECT
      '{{ exposed_type }}' AS type,
      territory_1,
      territory_2,
      year,
      {% for c in period.cols %}{{ c }},{% endfor %}
      carpools             AS journeys,
      passenger_seats      AS passengers,
      distance,
      duration
    FROM {{ ref('od_' ~ grain ~ '_' ~ model_type) }}
    {% if exposed_incremental() %}
      WHERE {{ period.key }} >= (SELECT lookback.{{ period.min }} FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  od.year,
  {% for c in period.cols %}od.{{ c }},{% endfor %}
  od.type,
  od.territory_1,
  p1.libelle                     AS l_territory_1,
  od.territory_2,
  p2.libelle                     AS l_territory_2,
  od.journeys,
  od.passengers,
  round(od.distance / 1000.0, 2) AS distance,
  round(od.duration / 60.0, 2)   AS duration,
  st_x(p1.centroid)              AS lng_1,
  st_y(p1.centroid)              AS lat_1,
  st_x(p2.centroid)              AS lng_2,
  st_y(p2.centroid)              AS lat_2
FROM od
LEFT JOIN {{ perimeters_agg_all() }} AS p1
  ON
    od.territory_1 = p1.code AND od.type = p1.type
    AND p1.year = least(od.year, (SELECT y FROM max_perim_year))
LEFT JOIN {{ perimeters_agg_all() }}
  AS p2 ON od.territory_2 = p2.code AND od.type = p2.type
AND p2.year = least(od.year, (SELECT y FROM max_perim_year))

{% endmacro %}
