# Migrations

## Prérequis

- `psql`, `pg_dump`, `pg_restore` (`geo-import`)
- `sha256sum`

## Commandes disponibles

- `just migrate` : joue toutes les migrations ; une base neuve a un schéma `geo` vide, la remplir avec `geo-import`
- `just seed` : joue toutes les migrations et charge les données de test de `providers/migration/seeds`
- `just geo-import <fichier> [sha256]` : remplace `geo.perimeters` et met à jour `com_evolution` depuis le datalake

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

`geo.perimeters` contient les 2 derniers millésimes exportés du datalake (`zone_trusted.perimeters`),
avec `valid_from` / `valid_until`. Une recherche sur une année plus ancienne
(`geo.get_latest_millesime_or`) retombe sur le dernier millésime.

```shell
# datalake : 2 derniers millésimes (--year pour choisir) + com_evolution
just export-perimeters
# api (psql, pg_restore >= version du serveur)
just geo-import ../datalake/tmp/geo/perimeters_2025-2026.<ts>.pgdump
```

`geo-import` appelle `src/db/geo/import.sh run` : vérification du dump contre son `.sha256` (`verify`),
restauration dans le schéma `geo_export` (`stage`), garde-fou et rapport (`check.sql`), puis
`import.sql` dans la même transaction (`apply`) :

- `geo_export.perimeters` remplace `geo.perimeters`. Au premier import, l'ancienne table est gardée
  en `geo.perimeters_legacy` (sauvegarde, plus jamais lue) ; ensuite la table remplacée est gardée en
  `geo.perimeters_prev` (retour arrière = deux `RENAME`) ;
- refus si le dernier millésime importé est plus ancien que celui en service ou a moins de 90 % de
  ses lignes ;
- `geo.com_evolution` (sans versions) : les années couvertes par l'export (depuis 2020) sont
  remplacées, les plus anciennes (2019) conservées.

En prod, le script tourne depuis l'image `docker/geo-import/prod` (psql + pg_restore 17), étape par
étape, avec le seul nom du dump : voir `src/db/geo/RUNBOOK.md`. Tests du script (sans base) :
`bash src/db/geo/import.test.sh`.
