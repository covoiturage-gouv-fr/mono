# Migrations

## Requirements

- `pg_dump`
- `pg_restore`
- `7z`
- `sha256sum`
- Access to the S3 bucket (Scaleway: geo-datasets-archives) (public read)

## Available commands

- `just migrate`: run all migrations and flash data from the cache
- `just seed`: run all migrations and seed test data from `providers/migration/seeds`
- `just source`: import datasets from `db/geo` to `geo.perimeters` (legacy, superseded by the datalake)
- `just geo-import <file|url> <sha256> [replace]`: import the millesimes and `com_evolution` dumped by the datalake
- `just external_data_migrate`: import external datasets

## Migrations

Migrations are ordered by name in the `src/db/migrations` folder.

- `000`: initial migrations (manual), e.g. `extensions.sql`
- `050`: geo schema
- `100`: application
- `200`: fraud
- `400`: observatory
- `500`: cee
- `600`: stats

## Dump all schemas

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

## Geo perimeters and millesimes

| Object | Content |
| ------ | ------- |
| `geo.perimeters_all` | millesimes imported from the datalake, partitioned by `year` |
| `geo.perimeters_<year>` | one partition per imported millesime |

`geo.perimeters` stays the current table until the first `geo-import`. At the end of it, in the
same transaction, `src/db/geo/switch-to-millesimes.sql` (no-op afterwards):

- renames the table to `geo.perimeters_legacy` (backup, never read again),
- creates the `geo.perimeters` view on the latest millesime of `geo.perimeters_all`,
- repoints the SQL functions reading `geo.perimeters` to `geo.perimeters_all`.

Lookups on a past year (trip date, APDF, campaigns) go through `geo.*` functions
(`geo.get_by_code`, `geo.get_latest_millesime_or`), which read the right table before and after the
switch. `valid_from` / `valid_until` come from the datalake (`zone_trusted.perimeters`).

Import from the datalake. The export holds the 2 latest millesimes so the previous one gets its
updated `valid_until`, hence `replace` = true. The dump is attached in one transaction.

```shell
# datalake
just export-perimeters
# api (psql, pg_restore >= server version)
just geo-import perimeters_2025-2026.<ts>.pgdump <sha256> true
```

The same transaction refreshes `geo.com_evolution` (no versioning): rows of the years covered by the
export (from 2020) are replaced, older ones (2019) are kept (`src/db/geo/import-com-evolution.sql`).

`geo.attach_millesime(source, year, replace)` copies the staging `source` rows for `year` into
`geo.perimeters_<year>` and attaches it. It refuses to:

- overwrite an existing millesime unless `replace` is true,
- attach a latest millesime with fewer than 90 % of the rows of the one in service.

`just source` (legacy geo pipeline) writes to the `geo.perimeters` table: do not use it after the
switch.

## Dump data for flashing

The `geo.perimeters` table can be sourced (see below) or flashed from a data dump.

The current archive fills the `geo.perimeters` table (before the switch). Do not rebuild it from a
switched database: run `geo-import` on the fresh database instead.

```shell
# dump geo data for flashing
DUMP_FILE=$(date +%F)_data.sql.7z
pg_dump -Fc -xO -a -n geo | 7z a -si $DUMP_FILE
sha256sum $DUMP_FILE | tee $DUMP_FILE.sha
```

1. Upload the archive alongside the sha256sum file to the cache bucket
   (geo-datasets-archives) using the web interface
2. Set the visilibity of both files to public
3. Update the cache configuration in the `api/src/db/cmd-migrate.ts` file
   with the public URL and the SHA256 checksum.
