{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'occupation']
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
  SELECT max(year) AS y FROM {{ ref('perimeters_agg') }}
),

territory AS (
  {% for model_type, exposed_type in type_map %}
    SELECT
      '{{ exposed_type }}' AS type,
      code,
      year,
      carpools             AS journeys,
      trips,
      carpools - no_oi     AS has_incentive,
      carpools,
      passenger_seats
    FROM {{ ref('territory_year_' ~ model_type ~ '_both') }}
    {% if is_incremental() %}
      WHERE year >= (SELECT lookback.min_year FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.type,
  t.code,
  p.libelle,
  t.journeys,
  t.trips,
  t.has_incentive,
  round(
    (t.carpools + t.passenger_seats)::numeric / nullif(t.carpools, 0), 2
  )                             AS occupation_rate,
  st_asgeojson(p.geom, 6)::json AS geom
FROM territory AS t
LEFT JOIN {{ ref('perimeters_agg') }} AS p
  ON
    t.code = p.code AND t.type = p.type
    AND p.year = least(t.year, (SELECT y FROM max_perim_year))
WHERE t.carpools > 0
