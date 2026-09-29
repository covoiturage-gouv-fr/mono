# Migrations

## Prérequis

- `pg_dump`
- `pg_restore`
- `7z`
- `sha256sum`
- Accès au bucket S3 (Scaleway : geo-datasets-archives), en lecture publique

## Commandes disponibles

- `just migrate` : joue toutes les migrations et flashe les données depuis le cache
- `just seed` : joue toutes les migrations et charge les données de test de `providers/migration/seeds`
- `just source` : importe les jeux de données de `db/geo` dans `geo.perimeters` (legacy, remplacé par le datalake)
- `just geo-import <fichier|url> <sha256> [replace]` : importe les millésimes et `com_evolution` exportés du datalake
- `just external_data_migrate` : importe les jeux de données externes

## Migrations

Les migrations sont jouées dans l'ordre de leur nom, dans le dossier `src/db/migrations`.

- `000` : migrations initiales (manuelles), ex. `extensions.sql`
- `050` : schéma geo
- `100` : application
- `200` : fraude
- `400` : observatoire
- `500` : CEE
- `600` : stats

## Dump de tous les schémas

```shell
pg_dump --no-owner --no-acl --no-comments -s -n geo > geo.sql
pg_dump --no-owner --no-acl --no-comments -s \
   -n dashboard_stats -n geo_stats -n observatoire_stats -n observatory \
    > observatory.sql

pg_dump --no-owner --no-acl --no-comments -s -n anomaly -n fraud -n fraudcheck > fraud.sql

pg_dump --no-owner --no-acl --no-comments -s \
   -n application -n auth -n carpool_v2 -n common -n company \
   -n export -n honor -n operator -n policy -n territory \
    > application.sql
```

## Périmètres géographiques et millésimes

| Objet | Contenu |
| ----- | ------- |
| `geo.perimeters_all` | millésimes importés du datalake, partitionnés par `year` |
| `geo.perimeters_<année>` | une partition par millésime importé |

`geo.perimeters` reste la table actuelle jusqu'au premier `geo-import`. À la fin de celui-ci, dans
la même transaction, `src/db/geo/switch-to-millesimes.sql` (sans effet ensuite) :

- renomme la table en `geo.perimeters_legacy` (sauvegarde, plus jamais lue) ;
- crée la vue `geo.perimeters` sur le dernier millésime de `geo.perimeters_all` ;
- repointe vers `geo.perimeters_all` les fonctions SQL qui lisaient `geo.perimeters`.

Les recherches sur une année passée (date du trajet, APDF, campagnes) passent par les fonctions
`geo.*` (`geo.get_by_code`, `geo.get_latest_millesime_or`), qui lisent la bonne table avant comme
après la bascule. `valid_from` / `valid_until` viennent du datalake (`zone_trusted.perimeters`).

Import depuis le datalake. L'export contient les 2 derniers millésimes : le précédent reçoit ainsi
son `valid_until` à jour, d'où `replace` = true. Tout le dump est attaché en une transaction.

```shell
# datalake
just export-perimeters
# api (psql, pg_restore >= version du serveur)
just geo-import perimeters_2025-2026.<ts>.pgdump <sha256> true
```

La même transaction met à jour `geo.com_evolution` (sans versions) : les lignes des années couvertes
par l'export (depuis 2020) sont remplacées, les plus anciennes (2019) sont conservées
(`src/db/geo/import-com-evolution.sql`).

`geo.attach_millesime(source, year, replace)` copie les lignes `year` de la table de transit
`source` dans `geo.perimeters_<année>` et l'attache. Elle refuse :

- d'écraser un millésime existant, sauf si `replace` vaut true ;
- d'attacher un dernier millésime qui a moins de 90 % des lignes de celui en service.

`just source` (pipeline géo legacy) écrit dans la table `geo.perimeters` : ne plus l'utiliser après
la bascule.

## Dump des données pour le flash

La table `geo.perimeters` peut être alimentée par `just source` ou flashée depuis un dump de données.

L'archive actuelle remplit la table `geo.perimeters` (avant la bascule). Ne pas la régénérer depuis
une base déjà basculée : lancer plutôt `geo-import` sur la base neuve.

```shell
# dump des données geo pour le flash
DUMP_FILE=$(date +%F)_data.sql.7z
pg_dump -Fc -xO -a -n geo | 7z a -si $DUMP_FILE
sha256sum $DUMP_FILE | tee $DUMP_FILE.sha
```

1. Déposer l'archive et le fichier sha256sum dans le bucket de cache (geo-datasets-archives) via
   l'interface web.
2. Rendre les deux fichiers publics.
3. Mettre à jour la configuration du cache dans `api/src/db/cmd-migrate.ts` avec l'URL publique et
   l'empreinte SHA256.
