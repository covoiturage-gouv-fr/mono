{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['year', 'month', 'type', 'code', 'direction'],
  indexes=[
    {'columns': ['year', 'month', 'type', 'code', 'direction'], 'unique': true}
  ],
  tags=['exposed', 'observatory', 'distribution']
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
] %}

WITH
{% if is_incremental() %}
  lookback AS (
    SELECT max(year * 100 + month) - 1 AS min_ym FROM {{ this }}
  ),
{% endif %}

max_perim_year AS (
  SELECT max(year) AS y FROM {{ ref('perimeters_agg') }}
),

distribution AS (
  {% for model_type, exposed_type, direction in type_direction_map %}
    SELECT
      '{{ exposed_type }}' AS type,
      '{{ direction }}'    AS direction,
      code,
      year,
      month,
      hours_distribution,
      dist_distribution
    FROM {{ ref('territory_month_' ~ model_type ~ '_' ~ direction) }}
    {% if is_incremental() %}
      WHERE year * 100 + month >= (SELECT lookback.min_ym FROM lookback)
    {% endif %}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT
  d.year,
  d.month,
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
    jsonb_build_object(
      'dist_classes',
      '0-10',
      'journeys',
      d.dist_distribution[1] + d.dist_distribution[2]
    ),
    jsonb_build_object(
      'dist_classes',
      '10-20',
      'journeys',
      d.dist_distribution[3] + d.dist_distribution[4]
    ),
    jsonb_build_object(
      'dist_classes',
      '20-30',
      'journeys',
      d.dist_distribution[5] + d.dist_distribution[6]
    ),
    jsonb_build_object(
      'dist_classes',
      '30-40',
      'journeys',
      d.dist_distribution[7] + d.dist_distribution[8]
    ),
    jsonb_build_object(
      'dist_classes',
      '40-50',
      'journeys',
      d.dist_distribution[9] + d.dist_distribution[10]
    ),
    jsonb_build_object(
      'dist_classes',
      '>50',
      'journeys',
      d.dist_distribution[11] + d.dist_distribution[12]
      + d.dist_distribution[13] + d.dist_distribution[14]
      + d.dist_distribution[15] + d.dist_distribution[16]
      + d.dist_distribution[17]
    )
  ) AS distances
FROM distribution AS d
LEFT JOIN {{ ref('perimeters_agg') }} AS p
  ON
    d.code = p.code AND d.type = p.type
    AND p.year = least(d.year, (SELECT y FROM max_perim_year))
WHERE d.code IS NOT NULL
