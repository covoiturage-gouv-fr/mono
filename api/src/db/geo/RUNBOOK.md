# Runbook — import annuel des périmètres geo

Remplace `geo.perimeters` par les 2 derniers millésimes exportés du datalake et rafraîchit
`geo.com_evolution`. Une fois par an, après `just export-perimeters` côté datalake (fichier sur le
bucket S3 `geo-datasets-archives`, dossier `geo/`, sha256 affiché par l'export).

Outils : image `ghcr.io/covoiturage-gouv-fr/geo-import` (`psql` + `pg_restore` 17, `curl`,
`sha256sum`, ce dossier dans `/opt/geo-import`). Connexion via `APP_POSTGRES_URL` (utilisateur de
l'API : propriétaire des tables `geo`). Le script est `geo-import` dans le PATH.

## Déroulé

Shell dans un pod monté sur cette image, puis :

```shell
URL=https://<bucket>/geo/perimeters_2025-2026.<ts>.pgdump
SHA=<sha256 affiché par l'export>

# 1. restaure le dump dans le schéma de travail geo_export — ne touche pas à geo
geo-import stage "$URL" "$SHA"

# 2. garde-fou + rapport : millésimes, lignes, dates de validité, lignes sans géométrie, com_evolution
geo-import check

# 3. remplacement, en une transaction (check rejoué au début) : tout ou rien
geo-import apply

# 4. ménage
geo-import cleanup
```

`geo-import run "$URL" "$SHA"` enchaîne les 4 étapes sans pause.

## Ce que `check` doit montrer avant `apply`

- deux millésimes, le dernier = l'année en cours ;
- `rows` du dernier millésime ≥ 90 % de celui en service (sinon `apply` refuse) ;
- `sans_geometrie` = 0 ;
- `valid_until` du dernier millésime = 1er janvier de l'année suivante ;
- `com_evolution` : une ligne par année depuis 2020.

Entre `stage` et `apply`, `geo_export.perimeters` est consultable en lecture (Metabase, `psql`).

## Vérifications après `apply`

```sql
SELECT geo.get_latest_millesime();                      -- = dernier millésime importé
SELECT * FROM geo.get_latest_by_point(2.3522, 48.8566); -- Paris
SELECT year, count(*) FROM geo.perimeters GROUP BY 1;   -- 2 millésimes
```

## Pannes et retour arrière

| Symptôme | Cause | Action |
| --- | --- | --- |
| `sha256 inattendu pour …` | mauvais fichier ou sha | reprendre URL et sha depuis la sortie de `just export-perimeters` |
| `pg_restore: error: unsupported version` | `pg_dump` du datalake plus récent que l'image | rebuild de l'image avec la version majeure du datalake |
| `check` : `millésime … refusé` | export tronqué ou plus ancien que la prod | ne pas forcer ; corriger l'export |
| `apply` : `canceling statement due to lock timeout` | une requête longue tient `geo.perimeters` (30 s) | relancer `apply` ; rien n'a été modifié |
| `apply` échoue pour toute autre raison | — | transaction annulée, rien à faire ; `geo_export` reste en place, `stage` le réécrit |

`apply` réussi mais mauvaises données : la table remplacée est conservée en `geo.perimeters_prev`
(`geo.perimeters_legacy` au tout premier import) :

```sql
BEGIN;
ALTER TABLE geo.perimeters RENAME TO perimeters_bad;
ALTER TABLE geo.perimeters_prev RENAME TO perimeters;
COMMIT;
```

puis `DROP TABLE geo.perimeters_bad` une fois le diagnostic fait. `geo.com_evolution` n'a pas de copie :
le ré-import d'un dump précédent la remet d'équerre.

## En local

```shell
cd api && just geo-import tmp/geo/perimeters_2025-2026.<ts>.pgdump <sha256>
```

(`APP_POSTGRES_URL` du `.env`, hôte `postgres` réécrit en `127.0.0.1`.)
