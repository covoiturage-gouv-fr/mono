{{ config(
  materialized='table',
  tags=['trusted', 'geo', 'com_evolution'],
) }}

SELECT
  mod,
  old_com,
  new_com,
  ROW_NUMBER() OVER ()                   AS id,
  DATE_PART('year', date_eff::date)::int AS year,  -- noqa: RF04
  CASE
    WHEN mod = 20 THEN 'création'
    WHEN mod = 21 THEN 'rétablissement'
    WHEN mod = 30 THEN 'suppression'
    WHEN mod = 31 THEN 'fusion simple'
    WHEN mod = 32 THEN 'création de commune nouvelle'
    WHEN mod = 33 THEN 'fusion association'
    WHEN mod = 41 THEN 'Changement de code dû à un changement de département'
    WHEN mod = 50 THEN 'Changement de code dû à un transfert de chef-lieu'
  END::varchar                           AS l_mod
FROM {{ source('raw', 'insee_mvt_com_2025') }}
WHERE
  date_eff::date >= '2020-01-01'
  AND mod IN (20, 21, 30, 31, 32, 33, 41, 50)
  AND typecom_ap = 'COM'
  AND typecom_av = 'COM'
ORDER BY date_eff, new_com
