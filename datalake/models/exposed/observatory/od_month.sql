{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'month', 'type', 'territory_1', 'territory_2'],
  indexes=[
    {'columns': ['year', 'month', 'type', 'territory_1', 'territory_2'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'od']
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
  SELECT max(year * 100 + month) - 1 AS min_ym FROM {{ this }}
),
{% endif %}

max_perim_year AS (
  SELECT MAX(year) AS y FROM {{ ref('perimeters_agg') }}
),

od AS (
  {% for model_type, exposed_type in type_map %}
    SELECT
      '{{ exposed_type }}' AS type,
      territory_1,
      territory_2,
      year,
      month,
      carpools             AS journeys,
      passenger_seats      AS passengers,
      distance,
      duration
    FROM {{ ref('od_month_' ~ model_type) }}
    {% if is_incremental() %}
  WHERE year * 100 + month >= (SELECT min_ym FROM lookback)
  {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  od.year,
  od.month,
  od.type,
  od.territory_1,
  p1.libelle                     AS l_territory_1,
  od.territory_2,
  p2.libelle                     AS l_territory_2,
  od.journeys,
  od.passengers,
  ROUND(od.distance / 1000.0, 2) AS distance,
  ROUND(od.duration / 60.0, 2)   AS duration,
  ST_X(p1.centroid)              AS lng_1,
  ST_Y(p1.centroid)              AS lat_1,
  ST_X(p2.centroid)              AS lng_2,
  ST_Y(p2.centroid)              AS lat_2
FROM od
LEFT JOIN {{ ref('perimeters_agg') }} AS p1
  ON
    od.territory_1 = p1.code AND od.type = p1.type
    AND p1.year = LEAST(od.year, (SELECT y FROM max_perim_year))
LEFT JOIN {{ ref('perimeters_agg') }}
  AS p2 ON od.territory_2 = p2.code AND od.type = p2.type
AND p2.year = LEAST(od.year, (SELECT y FROM max_perim_year))
