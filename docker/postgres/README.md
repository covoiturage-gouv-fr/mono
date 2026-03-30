# PostgreSQL Docker Image

Custom PostgreSQL 16 image based on `postgis/postgis:16-3.5-alpine` with the [h3-pg](https://github.com/zachasme/h3-pg) extension compiled from source.

## Extensions

- **PostGIS** 3.5 (from base image)
- **H3** spatial indexing (compiled from [h3-pg v4.2.3](https://github.com/zachasme/h3-pg/tree/a26630b8353d441e6bc8065c0a8dcaa3d89ef87b))

## Performance Tuning (local dev)

The default PostgreSQL configuration is very conservative. For local development (especially SQLMesh plans), performance settings are applied via `ALTER SYSTEM SET` and persisted in `postgresql.auto.conf` on the data volume (`./db/postgres`).

### Applied settings

**Parallelism** (adapt to your core count):

| Setting | Default | Tuned | Notes |
| ------- | ------- | ----- | ----- |
| `max_worker_processes` | 8 | 20 | Total background workers (= core count) |
| `max_parallel_workers` | 8 | 20 | Max workers for parallel queries |
| `max_parallel_workers_per_gather` | 2 | 10 | Workers per query (half core count) |
| `max_parallel_maintenance_workers` | 2 | 4 | For CREATE INDEX, VACUUM |
| `parallel_tuple_cost` | 0.01 | 0.001 | Lower = more eager to parallelize |
| `parallel_setup_cost` | 1000 | 100 | Lower = parallelize smaller queries |
| `min_parallel_table_scan_size` | 8MB | 1MB | Lower threshold for parallel scans |

**Memory**:

| Setting | Default | Tuned | Notes |
| ------- | ------- | ----- | ----- |
| `shared_buffers` | 128MB | 4GB | ~25% of available RAM |
| `effective_cache_size` | 4GB | 12GB | ~75% of available RAM |
| `work_mem` | 4MB | 256MB | Per-sort/hash operation |
| `maintenance_work_mem` | 64MB | 1GB | For VACUUM, CREATE INDEX |
| `hash_mem_multiplier` | 2.0 | 2.0 | Extra memory for hash joins |

**WAL / Write performance**:

| Setting | Default | Tuned | Notes |
| ------- | ------- | ----- | ----- |
| `wal_buffers` | -1 (auto) | 64MB | Write-ahead log buffer |
| `checkpoint_completion_target` | 0.9 | 0.9 | Spread checkpoint I/O |
| `max_wal_size` | 1GB | 4GB | WAL before forced checkpoint |

**Durability trade-offs (local dev only, DO NOT use in production)**:

| Setting | Default | Tuned | Notes |
| ------- | ------- | ----- | ----- |
| `synchronous_commit` | on | off | Don't wait for WAL flush |
| `fsync` | on | off | Skip fsync calls |
| `full_page_writes` | on | off | Skip full-page WAL writes |

### How to apply

Settings are already applied if you use the shared data volume. To re-apply or modify:

```bash
docker compose exec postgres psql -U postgres -c "ALTER SYSTEM SET work_mem = '256MB';"
docker compose exec postgres psql -U postgres -c "SELECT pg_reload_conf();"

# Some settings (shared_buffers, max_worker_processes) require a restart:
docker compose restart postgres
```

### How to reset

To revert to PostgreSQL defaults:

```bash
docker compose exec postgres psql -U postgres -c "ALTER SYSTEM RESET ALL;"
docker compose restart postgres
```

Or delete the data volume and recreate the container:

```bash
rm -rf db/postgres
docker compose up -d postgres
```
