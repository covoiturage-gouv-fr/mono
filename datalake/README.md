# Datalake

dbt project for the RPC analytics datalake. Replaces the legacy `sqlmesh/` project.
Uses Python 3.13 + uv + dbt-postgres + Elementary for data quality.

## Local development

**Prerequisites:** Python 3.13, uv, Docker, direnv

**Setup:**

1. `direnv allow` -- activates the virtual environment automatically on `cd`
2. `cp .env.example .env` and fill in the vars (see `.env.example`)
3. Run `just` to list all available commands

**Local Postgres:** the `postgres` service has no profile gate and starts with the default compose stack:

```bash
docker compose up -d postgres
```

## CI/CD

Two GitHub Actions workflows handle validation and deployment.

### PR validation (`.github/workflows/quality-datalake.yml`)

Runs on every pull request touching `datalake/`. Four parallel jobs:

- `uv lock` check -- ensures the lockfile is up to date
- Dockerfile build -- verifies the production image builds
- `dbt parse` -- validates model syntax and references
- sqlfluff lint -- enforces SQL style

### Deploy (`.github/workflows/deploy-datalake.yml`)

Triggers on push to `main` (production) or `datalake` (staging) when files under
`datalake/` or `docker/datalake/` change. Builds and pushes a Docker image to:

```
ghcr.io/betagouv/preuve-covoiturage/datalake:YYYY-MM-DD.HHMM
```

FluxCD deployment to Kubernetes is managed in the ops repository (separate from this repo).

## Runtime env vars

See `.env.example` for the full list.

## dbt commands

Generate a sources YAML from a database table:

```bash
dbt run-operation generate_source --args '{"schema_name": "fraudcheck", "database_name": "local","generate_columns": true, "table_names":["labels"]}' --profiles-dir ./profiles/
```

Generate a model YAML from an existing model:

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["interoperators_labels_by_month"]}' --profiles-dir ./profiles/
```

See `justfile` for pipeline orchestration commands (`just --list`).
