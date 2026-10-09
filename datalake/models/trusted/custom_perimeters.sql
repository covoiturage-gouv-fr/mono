{{ config(
  materialized='table',
  tags=['trusted', 'custom', 'daily'],
  indexes=[
    { 'columns': ['arr'] },
    { 'columns': ['code', 'arr', 'valid_from'], 'unique': true },
  ]
) }}

-- Territoires custom = territoires prod ayant des versions de périmètre
-- (CLI territory:perimeter). Même règle que territory.get_arr : à une date,
-- la version la plus haute dont [valid_from, valid_to) contient la date.
-- On découpe la frise aux bornes de toutes les versions pour obtenir des
-- intervalles disjoints, avec une version gagnante chacun.

WITH versions AS (
  SELECT
    tp.territory_id,
    tp.version,
    tp.arr,
    tp.valid_from,
    COALESCE(tp.valid_to, 'infinity'::timestamptz) AS valid_to
  FROM {{ source('dlk_import', 'territory_territory_perimeters') }} AS tp
  INNER JOIN {{ source('dlk_import', 'territory_territory_group') }} AS g
    ON tp.territory_id = g._id
  WHERE g.deleted_at IS NULL
),

bounds AS (
  SELECT
    territory_id,
    valid_from AS bound
  FROM versions
  UNION
  SELECT
    territory_id,
    valid_to AS bound
  FROM versions
),

segments AS (
  SELECT
    territory_id,
    bound                                                       AS seg_from,
    LEAD(bound) OVER (PARTITION BY territory_id ORDER BY bound) AS seg_to
  FROM bounds
),

winners AS (
  SELECT DISTINCT ON (s.territory_id, s.seg_from)
    s.territory_id,
    s.seg_from,
    s.seg_to,
    v.version,
    v.arr
  FROM segments AS s
  INNER JOIN versions AS v
    ON
      s.territory_id = v.territory_id
      AND s.seg_from >= v.valid_from
      AND s.seg_from < v.valid_to
  WHERE s.seg_to IS NOT NULL
  ORDER BY s.territory_id ASC, s.seg_from ASC, v.version DESC
),

members AS (
  SELECT DISTINCT
    territory_id,
    version,
    seg_from,
    seg_to,
    UNNEST(arr) AS arr
  FROM winners
)

SELECT
  territory_id::varchar AS code,
  version,
  arr::varchar          AS arr,
  seg_from              AS valid_from,
  seg_to                AS valid_until
FROM members
