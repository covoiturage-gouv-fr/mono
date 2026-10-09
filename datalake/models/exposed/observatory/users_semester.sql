{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'semester', 'type', 'code'],
  indexes=[
    {'columns': ['year', 'semester', 'type', 'code'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'users'],
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
  {% for model_type, exposed_type in exposed_type_map(type_map) %}
    SELECT
      '{{ exposed_type }}' AS type,
      code,
      year,
      semester,
      unique_drivers,
      new_drivers,
      unique_passengers,
      new_passengers
    FROM {{ ref('territory_semester_' ~ model_type ~ '_both') }}
    {% if exposed_incremental() %}
      WHERE year * 2 + semester >= (SELECT lookback.min_ys FROM lookback)
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
  t.unique_drivers,
  t.new_drivers,
  t.unique_passengers,
  t.new_passengers
FROM territory AS t
LEFT JOIN {{ perimeters_agg_all() }}
  AS p ON t.code = p.code AND t.type = p.type
AND p.year = least(t.year, (SELECT y FROM max_perim_year))
