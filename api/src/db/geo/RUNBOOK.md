# Runbook — import annuel des périmètres geo

Remplace `geo.perimeters` par les 2 derniers millésimes exportés du datalake et rafraîchit
`geo.com_evolution`. Une fois par an, après `just export-perimeters` côté datalake : le dump et son
`.sha256` sont déposés dans le bucket du datalake (`datalake-production`, dossier `geo/`), et l'export
affiche la commande d'import et le sha256.

Outils : image `ghcr.io/covoiturage-gouv-fr/geo-import` (`psql` + `pg_restore` 17, `curl`,
`sha256sum`, ce dossier dans `/opt/geo-import`). Connexion par les variables libpq `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGDATABASE` (et `PGSSLMODE`), avec l'utilisateur de l'API (propriétaire des
tables `geo`) ; à défaut `APP_POSTGRES_URL`, que le script éclate dans ces variables. Le mot de passe ne
passe jamais en argument de commande. Le script est `geo-import` dans le PATH.

Lecture S3 : `$GEO_IMPORT_S3_BASE_URL/geo/<nom du dump>` (origine fixée dans l'image), téléchargement
signé avec `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (clé en lecture seule sur `geo/*`, injectée
dans le pod), anonyme sans elles.

## Déroulé

Shell dans un pod monté sur cette image (`just run geo-import demo|production`, dépôt infra), puis :

```shell
DUMP=perimeters_2025-2026.<ts>.pgdump   # nom affiché par l'export

# 1. télécharge le dump et son .sha256, vérifie, restaure dans geo_export — ne touche pas à geo
#    (`geo-import verify "$DUMP"` fait la même chose sans toucher la base)
geo-import stage "$DUMP"

# 2. garde-fou + rapport : millésimes, lignes, dates de validité, lignes sans géométrie, com_evolution
geo-import check

# 3. remplacement, en une transaction (check rejoué au début) : tout ou rien
geo-import apply

# 4. ménage
geo-import cleanup
```

`geo-import run "$DUMP"` enchaîne les 4 étapes sans pause.

Le `.sha256` est lu dans le même bucket que le dump : il détecte un fichier tronqué ou corrompu, pas
une substitution. `stage` affiche le sha256 retenu : le comparer à celui de la sortie de l'export. Un
sha256 passé en second argument (`geo-import stage "$DUMP" <sha256>`) l'emporte sur le `.sha256`.

## Ce que `check` doit montrer avant `apply`

- deux millésimes, le dernier = l'année en cours ;
- `rows` du dernier millésime ≥ 90 % de celui en service (sinon `apply` refuse) ;
- `sans_geometrie` = 0 ;
- `valid_until` du dernier millésime = 1er janvier de l'année suivante ;
- `com_evolution` : une ligne par année depuis 2020.

Entre `stage` et `apply`, `geo_export.perimeters` est consultable en lecture (Metabase, ou `psql` sans
argument depuis le pod : les variables `PG*` suffisent).

## Vérifications après `apply`

```sql
SELECT geo.get_latest_millesime();                      -- = dernier millésime importé
SELECT * FROM geo.get_latest_by_point(2.3522, 48.8566); -- Paris
SELECT year, count(*) FROM geo.perimeters GROUP BY 1;   -- 2 millésimes
```

## Pannes et retour arrière

| Symptôme | Cause | Action |
| --- | --- | --- |
| `curl: (22) … 403` | clé S3 absente du pod, objet hors de `geo/`, ou URL complète en path-style | `env \| grep ^AWS_ACCESS` ; passer le seul nom du dump |
| `curl: (22) … 404` | nom du dump erroné, ou `.sha256` absent (export antérieur à son ajout) | reprendre le nom affiché par `just export-perimeters`, ou passer le sha256 en argument |
| `source inconnue` | ni nom de dump, ni fichier (`./…`), ni URL https | passer le nom exact, sans chemin |
| `sha256 mal formé` / `introuvable` | `.sha256` absent ou illisible | passer le sha256 en second argument |
| `sha256 inattendu pour …` | dump ou `.sha256` incohérents | reprendre le nom et le sha256 depuis la sortie de `just export-perimeters` |
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
cd api && just geo-import ../datalake/tmp/geo/perimeters_2025-2026.<ts>.pgdump
```

(`APP_POSTGRES_URL` du `.env`, hôte `postgres` réécrit en `127.0.0.1` ; sha256 lu dans le `.sha256`
écrit à côté du dump par l'export.)
