{{ config(
    materialized='view',
    tags=['exposed', 'observatory', 'infra']
) }}

-- Aires de covoiturage exposées à l'API (remplace la lecture directe de
-- `observatory.aires_covoiturage` + `geo.perimeters` côté API Deno).
--
-- L'API ne lit que la zone exposée. Cette vue :
--   * ne garde que les aires ouvertes (`ouvert = true`) ;
--   * reconstruit la géométrie GeoJSON depuis les coordonnées `Xlong`/`Ylat`
--     (la source ne fournit pas de colonne `geom`) ;
--   * conserve `insee` pour le filtrage territorial côté API.

SELECT
  id_lieu,
  nom_lieu,
  com_lieu,
  insee,
  type,
  date_maj,
  nbre_pl,
  nbre_pmr,
  duree,
  horaires,
  proprio,
  lumiere,
  ST_ASGEOJSON(
    ST_SETSRID(ST_MAKEPOINT("Xlong", "Ylat"), 4326), 6
  )::json AS geom
FROM {{ source('raw', 'aires_covoiturage') }}
WHERE ouvert = true
