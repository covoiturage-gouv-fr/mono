# Observatoire `location` — pré-agrégation (version robuste)

**Statut** : proposition. Mitigation immédiate déjà livrée (filtre sargable + `statement_timeout` 30 s).

## Problème

L'endpoint `/observatory/location` (heatmap de densité H3) agrège **à la volée**
`zone_trusted.carpools` : la vue `zone_exposed.location` expose les colonnes brutes et
`build_location_query` fait le binning `h3_cell_to_parent(start/end_h3index_z8, n)` +
`GROUP BY` à chaque requête.

- Avant fix : filtre `EXTRACT(YEAR/MONTH FROM start_datetime)` non-sargable → **Seq Scan
  de ~50 M lignes** → **~60 s** → coupé par `statement_timeout` (5 s) → **500**.
- Après mitigation (sargable) : Index Scan sur `start_datetime_tz` → **~2,4 s** pour une
  région-mois. Mais un scope **année** ou **pays** reste lourd (12× à N× plus de lignes),
  proche/au-delà du timeout, et chaque requête relit `carpools` (coûteux, tient une
  connexion du pool). Les autres stats (flux, occupation…) lisent des **tables
  pré-agrégées** et répondent en < 100 ms.

## Objectif

Aligner `location` sur le patron des autres stats : l'API lit une **table pré-agrégée**
de `zone_exposed`, petite et indexée, au lieu de scanner `carpools`.

## Design proposé

### Modèle dbt agrégé

Nouvelle table matérialisée (grain z8, le plus fin utilisé par le front qui envoie `n=8`) :

```
zone_aggregated.location_z8 (
  type            text,        -- grain territoire (com/epci/aom/dep/reg/country)
  code            text,        -- code du territoire (millésime courant des périmètres)
  year            int,
  month           int,         -- + colonnes grain (ou tables _month/_quarter/_semester/_year
                               --   comme od_*, au choix)
  h3_z8           h3index,     -- hexagone z8 (départ OU arrivée, dédoublonné par ligne)
  count           bigint
)
```

- Peuplée par un modèle dbt lisant `zone_trusted.carpools` (mêmes filtres :
  `valid_acquisition_status`, `start_datetime_tz < now()-2j`), croisé avec
  `zone_trusted.perimeters_agg` pour rattacher chaque trajet à ses territoires
  (start_geo_code / end_geo_code → type+code), puis `GROUP BY type, code, période, h3_z8`.
- Projetée en `zone_exposed.location_z8` (contrat public), avec `GRANT SELECT` à
  `datalake_api_ro` **via `ALTER DEFAULT PRIVILEGES`** (voir « Droits » plus bas).

### Requête API

`build_location_query` lit la table pré-agrégée :

```sql
SELECT h3_cell_to_parent(h3_z8, %(n)s)::text AS hex, sum(count)::int AS count
FROM zone_exposed.location_z8
WHERE type = %(type)s AND code = %(code)s
  AND <filtre période sargable>
GROUP BY 1
```

- Pour `n = 8` : `h3_cell_to_parent(h3_z8, 8) = h3_z8` (no-op) → lecture directe.
- Pour `n < 8` : agrégation vers l'ancêtre — sur un petit ensemble (un territoire × une
  période), pas sur `carpools`.
- Plus de jointure `carpools` ni de résolution de périmètre au runtime (faite en amont).

### Pipeline & droits

- Brancher le modèle dans le pipeline exposé (`pipeline-daily` / `models/exposed`), avec
  `cache-bump` pour l'invalidation Redis (déjà en place pour les autres agrégés).
- **Droits** : le bug 500 initial venait aussi de vues recréées perdant leur ACL. Poser
  un `ALTER DEFAULT PRIVILEGES IN SCHEMA zone_exposed GRANT SELECT ON TABLES TO
  datalake_api_ro` (et pour les vues) pour que les objets recréés par dbt gardent le grant.

### Suites

- Après passage en pré-agrégé : **rabaisser `db_statement_timeout_ms` à 5 s** (le 30 s
  n'était qu'un stopgap).
- Décider du grain exposé (`_month/_quarter/_semester/_year` séparés comme `od_*`, ou une
  table unique avec colonnes de grain). Cohérence avec `observatory/helpers/tableName.ts`
  côté front (le front ne fait que passer les params ; pas d'impact).

## Hors-scope

- Seuil de k-anonymat sur la heatmap (`count=1` à n=8 ré-identifiant) — hérité du legacy,
  à traiter séparément (déjà noté dans GEN-668).
