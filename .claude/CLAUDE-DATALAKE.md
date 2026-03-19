# SQLMesh — Agent Instructions

> **Read `sqlmesh/README.md` first.** It is the single source of truth for architecture, conventions, macros, and patterns. This file contains only agent-specific workflow guidance.

## Quick Setup

```bash
cd sqlmesh
uv sync
cp .env.example .env  # edit with local credentials
```

## Verification Workflow

After any model change, always run:

```bash
# 1. Check the rendered SQL is correct
sqlmesh render <model_name>

# 2. Preview the plan (never auto-apply without review)
sqlmesh plan dev
```

For production: `sqlmesh plan` (no env suffix).

## Key Rules (reminders — README is canonical)

- **Filter on `start_datetime` (UTC)**, never on `start_datetime_tz`
- **Use `@start_ts` / `@end_ts`** for timestamp filters, never `@start_ds` / `@end_ds`
- **Use `@create_index()`** macro, never raw `CREATE INDEX`
- **Model `start`/`end` must include timezone offset** (`+0100` for winter boundaries)
- File name must match model name in `MODEL` block

## PostgreSQL Live Tables (data sources for archive_zone)

```
carpool_v2.carpools          # Core journey data
carpool_v2.geo               # Geographic data
carpool_v2.status            # Acquisition status
policy.incentives            # Incentive data
operator.operators           # Operator metadata
company.companies            # Company info (SIRET)
```

## Python Utilities

| Module | Purpose |
|--------|---------|
| `utils/loading.py` | Load CSV, Excel, Parquet, GeoPackage |
| `utils/cleaning.py` | Column normalization, type casting |
| `utils/s3.py` | S3 client initialization |
| `utils/export_data.py` | Query export to CSV/Parquet |
| `utils/upload.py` | S3 multipart upload |
