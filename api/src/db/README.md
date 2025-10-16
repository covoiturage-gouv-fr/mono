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
- `just source`: import datasets from `db/geo` to `geo.perimeters`
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
pg_dump --no-owner --no-acl --no-comments -s -n cee > cee.sql
pg_dump --no-owner --no-acl --no-comments -s \
   -n dashboard_stats -n geo_stats -n observatoire_stats -n observatory \
    > observatory.sql

pg_dump --no-owner --no-acl --no-comments -s -n anomaly -n fraud -n fraudcheck > fraud.sql

pg_dump --no-owner --no-acl --no-comments -s \
   -n application -n auth -n carpool_v2 -n common -n company \
   -n export -n honor -n operator -n policy -n territory \
    > application.sql
```

## Dump data for flashing

The `geo.perimeters` table can be sourced (see below) or flashed from a data dump.

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
