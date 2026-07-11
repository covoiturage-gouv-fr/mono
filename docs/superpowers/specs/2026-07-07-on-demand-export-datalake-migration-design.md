# On-demand export migration: sqlmesh → datalake

**Date:** 2026-07-07
**Status:** Design — approved for planning
**Scope:** On-demand `export.exports` queue only. The monthly data.gouv.fr open-data publication is a **separate, later spec** (shares the `export_opendata` model + S3 plumbing but none of the control plane below).

## Problem

Today the API produces on-demand journey exports itself: the `export:process` command drains the `export.exports` queue, queries the **sqlmesh** view `refined_zone.export_carpool_list` (live, on the API DB), streams rows to a zipped CSV, uploads to the `export` S3 bucket, and emails the recipient. We are retiring sqlmesh in favour of the **datalake** (dbt, separate `datalake_production` DB, reads API data read-only through the FDW). File production must move off the API and onto the datalake, which already owns the equivalent model (`export_partners`).

## Principles / constraints

- **The FDW stays read-only, one-directional, PII-minimized** (as documented in `20260703000000-datalake-fdw-export.sql`). We do **not** add the `export` schema to it. The FDW is the *data plane* only.
- **The API remains the sole owner and writer of `export.exports`.** All control-plane writes go through the API over HTTP (the *control plane*).
- **The partners CSV contract must not drift.** Same columns, order, formatting, delimiter (`;`), and `.csv`-inside-`.zip` packaging as today.
- **YAGNI:** drop the unused `progress` mechanism entirely (column + code).

## Architecture

Two planes, cleanly separated:

- **Data plane (read-only, FDW):** the datalake reads carpool data through the existing `dlk_import` foreign tables. Unchanged.
- **Control plane (bidirectional, HTTP):** the datalake worker claims tasks and reports outcomes via authenticated API endpoints.

### Data freshness

The export query reads the datalake's **materialized trusted zone** (`ref('carpools')`, refreshed daily), **not** live via FDW. Deliberate trade-off: an export can miss journeys from the last few hours vs. today's live-view behaviour. Accepted as the point of the datalake; worth a one-line note to whoever signs off. (Note: `export_partners` still joins `dlk_import` live for per-row identity keys + GPS positions — static per-row attributes, so row-set freshness is unaffected.)

### On-demand flow (target)

```
User → POST /exports (unchanged) → export.exports row = 'pending'         [API DB]

Datalake CronJob (every N min, k8s, ops repo):
  1. POST /exports/claim {targets:[...]}
        → API: UPDATE export.exports SET status='running'
               WHERE status='pending' AND target = ANY(:targets)
               ORDER BY created_at ASC LIMIT 1
               RETURNING uuid, target, params          (atomic, race-free)
        → 204 if nothing to claim (worker exits)
  2. Build parameterized SQL over zone_exposed.export_partners with params
        (start/end date → start_date_filter, operator_id, geo_selector)
  3. Postgres COPY (SELECT … WHERE …) TO STDOUT (FORMAT CSV, DELIMITER ';', HEADER)
        → stream to local {uuid}.csv → zip → {uuid}.csv.zip
  4. Upload to S3 export bucket, key = "{uuid}.csv.zip"
  5. POST /exports/{uuid}/complete {file_size}
        → API: status='success', email recipients
     (on error) POST /exports/{uuid}/fail {message}
        → API: status='failure', error email to user + support

UI polls /exports/list — unchanged. Download presign — unchanged.
Stale reaper (API, existing): running > timeout → back to pending → reclaimed next tick.
```

## Components

### 1. API — control-plane endpoints (net-new)

**Auth (coherent with existing pattern):** a dedicated **Dex "system" application credential** (`DexClient.createForOperator`, `operator_id = 0`, a new least-privilege role/group) + a new `export.process` permission in `permissions.ts`, gated by `hasPermissionMiddleware("<group>.export.process")` — identical to how operators authenticate the acquisition API. The worker does `access_key/secret_key → POST /auth/access_token → Bearer JWT`; `authGuard`/`accessTokenMiddleware` verify via Dex JWKS. No shared secret, no new scheme. (Rejected: a static env bearer token — no inbound-auth precedent in the codebase; `internalOnlyMiddlewares` — it deliberately blocks the HTTP `proxy` channel.)

Three authenticated endpoints:

- `POST /exports/claim` — body `{ targets: string[] }`. Atomic `UPDATE … RETURNING` claim. Returns `{ uuid, target, params }` or `204`.
- `POST /exports/:uuid/complete` — body `{ file_size? }`. Sets `success`, emails recipients. Object key is derived as `{uuid}.csv.zip` (API already knows `uuid`; the API lists objects directly from the `export` bucket, so the key need not be passed back).
- `POST /exports/:uuid/fail` — body `{ message }`. Sets `failure`, records `error`, emails user + support.

Kept as-is: `POST /exports` (create), `GET /exports/download-link/:id` (60 s presign against `export` bucket), `POST /exports/list`, the stale-exports reaper.

### 2. Datalake — Python worker (net-new)

- `datalake/pipelines/cmd/export_worker.py` (Typer), `just export-worker` recipe, k8s CronJob (ops repo) every few minutes.
- Steps: claim → template SQL over `export_partners` → `psycopg` `COPY … TO STDOUT` streaming to file → `zipfile` → upload to `export` bucket → complete/fail.
- **Engine choice:** native Postgres `COPY`, not DuckDB. Data is already in `datalake_production`; `COPY` streams server-side with no extra indirection. (DuckDB is fast too but adds nothing here.) The `.zip` packaging is a Python `zipfile` step since neither PG nor DuckDB emit `.zip` natively.
- **S3 config via env vars** (`export` bucket endpoint + credentials); secrets wired in infra by the operator. The `export` bucket keeps its own lifecycle/expiry rules — the reason we write there rather than the datalake bucket.

### 3. Model / query coupling (highest risk)

**Prior art — this was already tried and reverted.** `#3208` switched the API export to the dbt views; it was reverted by `#3209`, then followed by `#3213` ("corrige les index manquants bloquant l'export data.gouv") and `#3220` ("expose start_datetime_utc dans l'export carpool"). The rollback was for **index/perf** reasons. This migration must not repeat it — hence the explicit performance section below.

- **Column contract — source of truth is the *current* sqlmesh view.** The contract is what the API emits **today**, which runs `refined_zone.export_carpool_list` (patched through `#3220`) shaped by API `config/export.ts` `fields` (CSV **column order + header names**) + `CarpoolRow` computed fields + `CSVWriter` transformations. In the new design the **worker's `SELECT` column order defines the CSV**, so it must reproduce the `fields` order exactly. Required blocking task: a **three-way diff** — (a) current sqlmesh `export_carpool_list` output, (b) API `fields` order/names/formatting, (c) datalake `export_partners`. Silent drift breaks partners' downstream parsers.
  - **Known gap:** `export_partners` exposes formatted `start_datetime` + `start_date_filter` (date) but **not** the raw `start_datetime_utc` (UTC) column. That column must be added — it is both part of the current contract intent and the key to the sargable predicate below.
- **geo_selector predicate:** the geo filtering logic in `carpoolListQuery.ts` must be **ported into the worker's SQL template** (start/end INSEE / AOM / EPCI / region predicates from `params.geo_selector`).
- `export_partners` row set is sourced from `ref('carpools')` (materialized trusted zone) per the freshness decision; re-point if any part still reads live `dlk_import` for the row set.

### 3b. Performance — indexes & sargable WHERE (why #3208 was reverted)

The worker query must be index-driven, not a table scan. Replicating today's live-view behaviour:

- **Sargable predicate on the raw UTC column.** Filter on `start_datetime_utc` (raw `start_datetime`, UTC) with the `AT TIME ZONE 'Europe/Paris'` bounds, **in addition to** the `start_date_filter` (::date) range and `ORDER BY start_date_filter ASC`. The UTC predicate is what hits the index; the ::date predicate is for exactness at the day boundary.
- **Indexes on `trusted.carpools`** must cover the filter columns: raw `start_datetime` (UTC), `operator_id`, and the geo codes (`start_geo_code` / `end_geo_code`) used by `geo_selector`. The datalake trusted model has **no index config found** — replicating the sqlmesh index fixes (`#3213`) on the datalake side is a blocking task, else exports table-scan and time out exactly as in the reverted attempt.
- **FDW join cost.** `export_partners` joins live `dlk_import` for identity keys + positions; ensure that join is on an indexed key (`_id`) and constrained by the date-bounded row set so it does not pull the whole foreign table. Validate the plan with `EXPLAIN` on a representative date range during the contract task.

### 4. Simplifications (this migration)

- **Drop `progress`:** migration to drop `export.exports.progress`; remove `ExportRepository.progress()`, the `progress` entry in the update whitelist, `Export.progress`, and `ExportProgress`/`FileCreatorService`/`CarpoolRepository` progress plumbing.
- **Delete API file production** (after cutover): `ProcessCommand`, `FileCreatorService`, `CSVWriter`, `CarpoolRow`, `carpoolListQuery`, `carpoolCountQuery`, and the sqlmesh `refined_zone.export_*` views once nothing reads them.

## Cutover — strangler by target

Live queue targets are **operator** and **territory** only (`datagouv` is an unreachable enum fallback; the `opendata` rows are legacy pre-rename values — ignorable). Order: operator first as the canary (lower volume, same "carpool" shape as territory), then territory (the bulk).

1. Ship the datalake worker + the 3 API endpoints. No deletions yet.
2. **Flag = target ownership.** Datalake worker claims `targets=[operator]` first. The API `ProcessCommand.pickPending` gets a temporary "exclude these targets" filter so the two workers never claim the same row. Both run in parallel.
3. Validate parity on a **settled historical range** (trusted == live), per target.
4. Add `territory`, then flip everything to the datalake.
5. Stop scheduling the API worker; delete the API file-production code + `progress` + `refined_zone.export_carpool_list` (NOT `export_opendata_list`).

## Error handling

- **Claim race:** eliminated by the atomic `UPDATE … WHERE status='pending' … RETURNING`.
- **Worker crash mid-job:** row stuck `running`; the existing API stale reaper returns it to `pending` for reclaim. Self-healing; safe because the worker writes to a deterministic key and only calls `complete` on success.
- **Query/upload failure:** worker calls `/fail`; API sets `failure` + emails.
- **API unreachable from worker:** claim/complete/fail are retried with backoff; an un-acked `complete` is safe to retry (idempotent on `uuid`).

## Testing

- **Unit (datalake):** SQL template builds correct predicates for date range / operator / each geo_selector shape; zip packaging produces `{uuid}.csv.zip` with `;`-delimited CSV + header.
- **Contract (blocking):** column-by-column diff `export_partners` vs. current API output on a fixed sample.
- **Integration (API):** claim atomicity under concurrent calls (no double-claim); complete/fail state transitions + emails; target-exclusion filter during strangler.
- **End-to-end (staging):** create export → datalake worker claims/produces/uploads → API marks success + emails → download presign resolves.

## Risks surfaced by design challenge

1. **The CSV is not the view — it's view + TS transforms + per-target exclusions.** `config/export.ts` defines **one field list but three per-target shapes** via `filters`: `OPERATOR` drops `operator`+`has_incentive`; `TERRITORY` drops `has_incentive`; `DATAGOUV` drops all operator/PII/incentive detail (the narrow open-data shape). The worker must reproduce **each target's exact column set and order**, not one CSV.
2. **Computed fields — investigated, largely de-risked.** `status` (enum cast), `has_incentive`, and `incentive_type` are computed in TS today, with no column in `export_partners`. Findings:
   - **`incentive_type` is INERT** — `export_carpool_list` never projects a `campaigns` id-array column, so the TS reducer runs over `[]` and returns **`"normal"` for every row**. To preserve parity, emit the constant `'normal'`. The "real" booster/mode logic needs `booster_dates`, which exists **only in hardcoded TS policy classes** (not in `policy.policies`, not in the FDW) — so fixing it is a separate data-plumbing project, explicitly **out of scope**. *(Empirically confirm "always normal" on a real prod export sample.)*
   - **`has_incentive`** — trivial SQL (`COALESCE(incentive_0/1/2_siret) IS NOT NULL` → OUI/NON); only in the `datagouv` shape, which is not a live on-demand target.
   - **`status`, `trip_id` hash, tz datetime formatting** — smaller; confirm exact mapping in the contract diff (task 2).
   - **Decision:** replicate the inert `'normal'` for exact parity (keep behavior as-is). Fixing `incentive_type` is deferred to a later data-plumbing project.
3. **Parity validation cannot be a naïve byte-diff.** Old path reads the live view; new path reads the daily trusted zone — rows *will* differ by design (freshness). Validate on a **settled historical range** (e.g. a fully-closed past month) where trusted == live, else the diff is meaningless.
4. **Throughput.** Today `export:process` drains **up to 50 per run**. A LIMIT-1-per-tick worker would starve under backlog — the worker must **loop-claim until 204** (with a wall-clock guard), or run with CronJob parallelism.
5. **Double-processing / double-email.** If the stale reaper returns a *still-running* (slow, not dead) job to `pending`, a second worker reprocesses it. Deterministic key makes the upload idempotent, but `complete` must be **idempotent and email exactly once** — guard the email on the `running→success` transition, skip if already `success`. Also set the stale timeout safely above max real export duration.
6. **Trusted-zone freshness — largely resolved by the existing end-date cap.** `config/export.ts` `maxEndDefault` is already **"5 days ago"**, and the business rule is a **48 h validation window** (journeys aren't fully validated for 48 h). So every export ends days in the past — comfortably within the daily refresh + 3-day lookback. The only residual is **mid-rebuild consistency** (an export claimed while the daily `delete+insert` runs): confirm the rebuild is transactional, or gate the worker during the daily window. Keep the existing end-date cap; no new cap needed.
7. **Network path.** The datalake (separate infra) must reach the API over the network for claim/complete/fail (confirm the route + TLS). *Auth is resolved — see Component 1: a Dex "system" application credential + `export.process` permission, coherent with the existing operator-credential pattern.*
8. **Do NOT delete `export_opendata_list` in this migration.** `datagouvListQuery.ts` still reads it and the monthly data.gouv publication stays on the API until the separate later spec. Task-8 deletion is limited to `export_carpool_list` (+ the count query, which dies with `progress`).
9. **Pod ephemeral disk.** Large exports write CSV + zip to local pod disk before upload — size the ephemeral storage or stream to S3 multipart.

## Contract diff — result (blocking task 2, resolved)

Ran the three-way diff against `datalake_production` on settled ranges (2023-08-07 = jour avec CEE, 2026-06-15 = sans). All 64 `operator`-target columns resolve, order + header names match `config/export.ts` `fields`. After parsing the CSV (quotes removed), **one** material value drift:

- **`cee_application`.** The API's `CSVWriter` casts the boolean via `csv-stringify` (default cast `value ? "1" : ""`) → **`1` / empty**. Raw `COPY … FORMAT CSV` renders a Postgres boolean as **`t` / `f`** — a real break for any partner parser matching on the value. **Fix:** `export_partners` now emits `CASE WHEN cee._id IS NOT NULL THEN '1' ELSE '' END` (column type `text`), guarded by `tests/exposed/export/export_partners_cee_application_format.sql`.
- **False positive — coordinates.** An apparent `6.47` vs `6.470` drift was an artefact of the validation harness (JSON round-trip coerces `numeric` to a JS number, dropping the trailing zero). The old sqlmesh model uses the identical `TRUNC(…::numeric, precision)`; node-postgres returns `numeric` as a string, so the live output is `6.470` too. No drift.
- **Accepted cosmetic delta — quoting.** `CSVWriter` sets `quoted_string: true` (every string cell + header name quoted); `COPY` quotes minimally. Both are valid RFC-4180 CSV with the same `;` delimiter, columns, order and values, so downstream CSV parsers are unaffected. Exact parity is not reproducible via `COPY` anyway (it never quotes the header). Left as-is; revisit with `FORCE_QUOTE` only if a partner needs byte-level quoting.

## Datalake DB migration runner (deploy-time)

dbt manages models (tables/views) but **not** the prerequisite DB objects the models assume — custom functions (`ts_ceil`), grants, etc. Local setup proved these are unmanaged (a fresh DB can't build the export models). A lightweight runner owns that layer:

- `datalake/pipelines/cmd/migrate.py` — applies ordered, idempotent `datalake/migrations/*.sql` via psycopg 3 (reuses the worker's driver, no new dep). Applied versions are tracked in `public.schema_migrations`; re-runs are no-ops. Each file runs atomically via the simple-query protocol (supports multi-statement + `$$`-quoted bodies); a failing file rolls back and is not recorded.
- `just migrate` wraps it. First migration: `0001_ts_ceil.sql`.
- **Trigger:** the ops repo (FluxCD) runs `just migrate` as a **k8s Job on each deployment**, before the app/pipeline pods start.
- **Boundary:** privileged / cross-DB plumbing — role creation (`datalake_fdw`), the FDW server + `IMPORT FOREIGN SCHEMA` — stays in the ops repo (OpenTofu, superuser + per-env secrets). The runner assumes those exist and owns only repo-portable, non-privileged DDL.
- `0000_fdw.sql` — **documentation-only** (comments, no executable SQL): describes the superuser FDW setup on both DBs, recorded in the ledger as a versioned placeholder. FDW password rotation (both the API-DB role and this DB's `USER MAPPING`) is owned by the infra / ops repo (OpenTofu) — not this repo — since it needs superuser + secret management.

## Out of scope

- Monthly data.gouv.fr open-data publication (separate later spec).
- Raw parquet archival (already migrated: `datalake/pipelines/cmd/export.py`).
- Consumer-side FDW plumbing and k8s CronJob manifests (ops repo).

## Open items for planning

- Confirm current archive is `.csv` in `.zip` with `;` delimiter across all live consumers (verified in code: `CSVWriter` `archiveExt='zip'`, `delimiter=';'`).
- Decide machine-auth scheme for the 3 endpoints (reuse operator-credential JWT vs. a dedicated service token).
- Whether `download_url` column becomes vestigial once the API lists objects directly (candidate follow-up cleanup, not required here).
- Confirm the exact index set that fixed `#3213` on the sqlmesh side and mirror it on `trusted.carpools`; capture `EXPLAIN` before/after on a representative export range.
