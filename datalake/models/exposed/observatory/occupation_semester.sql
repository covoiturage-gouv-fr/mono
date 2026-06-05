{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'semester', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'semester', 'type', 'code'], 'unique': true}
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
  SELECT max(year * 2 + semester) - 1 AS min_ys FROM {{ this }}
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
    semester,
    carpools              AS journeys,
    trips,
    carpools - no_oi      AS has_incentive,
    carpools,
    passenger_seats
  FROM {{ ref('territory_semester_' ~ model_type ~ '_both') }}
  {% if is_incremental() %}
  WHERE year * 2 + semester >= (SELECT min_ys FROM lookback)
  {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  t.year,
  t.semester,
  t.type,
  t.code,
  p.libelle,
  t.journeys,
  t.trips,
  t.has_incentive,
  round(
    (t.carpools + t.passenger_seats)::numeric / nullif(t.carpools, 0), 2
  )                                            AS occupation_rate,
  st_asgeojson(p.geom, 6)::json               AS geom
FROM territory t
LEFT JOIN {{ ref('perimeters_agg') }} p ON p.code = t.code AND p.type = t.type
  AND p.year = LEAST(t.year, (SELECT y FROM max_perim_year))
WHERE t.carpools > 0
