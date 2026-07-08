# On-demand Export Datalake Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move on-demand journey-export file generation off the API and onto the datalake, which reads carpool data locally, produces the CSV, uploads it to the `export` S3 bucket, and reports back to the API over an authenticated HTTP control plane.

**Architecture:** Two planes. The **data plane** stays as-is — the datalake reads carpool data from its own materialized `trusted`/`exposed` zone (fed by the read-only FDW). The **control plane** is net-new HTTP: a datalake Python worker claims a pending export via the API (atomic `UPDATE … RETURNING`), runs a parameterized DuckDB `COPY` over `zone_exposed.export_partners`, zips it, uploads to the `export` bucket, and calls the API to mark success/failure (which emails the recipient). The API remains the sole writer of `export.exports`.

**Tech Stack:** API — Deno 2.x, TypeScript, ILOS (Express + Inversify), tagged-template SQL. Datalake — dbt-core + dbt-postgres, Python 3.13 (uv), Typer CLI, DuckDB (Postgres attach + S3), boto3, requests. Auth — Dex OIDC application credentials.

## Global Constraints

- **CSV contract must not drift.** Semicolon (`;`) delimiter, header row, `.csv` inside a `.zip` (matches `CSVWriter` `archiveExt='zip'`, `delimiter=';'`). Per-target column set + order from `api/src/pdc/services/export/config/export.ts` `fields` minus `filters[target].exclusions`.
- **Live queue targets are `operator` and `territory` only.** `datagouv` is an unreachable enum fallback; the `opendata` rows are legacy. The worker handles `operator` + `territory`.
- **`incentive_type` is emitted as the constant `'normal'`** (parity with today's inert behavior). Do NOT reproduce campaign mode logic.
- **FDW stays read-only.** No writes from the datalake to the API DB; all control-plane writes go through the 3 HTTP endpoints.
- **The API remains the sole owner/writer of `export.exports`.**
- **Export end date is already capped days in the past** (`config/export.ts` `maxEndDefault`), so trusted-zone freshness is not a correctness risk. Do not add a new cap.
- **Never commit on `main`.** Work in the `worktree-mig-export` worktree; human review + Yubikey-signed commits.
- Spec: `docs/superpowers/specs/2026-07-07-on-demand-export-datalake-migration-design.md`.

---

## Phase 1 — Datalake model & indexes (foundation)

### Task 1: Add filter + parity columns to `export_partners`

**Files:**
- Modify: `datalake/models/exposed/export/export_partners.sql`
- Modify: `datalake/models/exposed/export/yml/_export_partners.yml`

**Interfaces:**
- Produces: the `export_partners` view now also exposes `start_datetime_tz` (raw `timestamp`, for sargable filtering), `start_datetime_utc` (raw UTC `timestamp`, filter parity), `incentive_type` (`text`, constant `'normal'`), and `status` (`text`, the CSV's single carpool status). These are consumed by the worker's SQL (Task 11) and the contract diff (Task 13).

- [ ] **Step 1: Add the filter + parity columns to the SELECT**

In `export_partners.sql`, add to the SELECT list (place `start_datetime_tz`/`start_datetime_utc` near the other date columns, and `incentive_type` right after `cee_application` to mirror the API `fields` order):

```sql
  -- raw timestamps for sargable filtering (not emitted in CSV)
  c.start_datetime_tz
    AS start_datetime_tz,
  c.start_datetime
    AS start_datetime_utc,
```

and after the `cee._id IS NOT NULL AS cee_application,` line, add:

```sql
  'normal'::text
    AS incentive_type,
```

- [ ] **Step 1b: Reproduce the `status` output column**

The API emits a single `status` column computed in TS by `castToStatusEnum` (`api/src/pdc/providers/carpool/helpers/castStatus.ts`) from the raw `acquisition_status`/`fraud_status`/`anomaly_status`. `export_partners` exposes those three raw columns but not the combined `status`. Read `castStatus.ts` and port its precedence logic to a SQL `CASE` in `export_partners.sql`, e.g.:

```sql
  -- combined carpool status (port of castToStatusEnum precedence)
  CASE
    WHEN c.acquisition_status <> 'processed' THEN c.acquisition_status
    WHEN c.fraud_status = 'failed'           THEN 'fraud'
    WHEN c.anomaly_status = 'failed'         THEN 'anomaly'
    ELSE 'valid'
  END
    AS status,
```

*(The exact string values + precedence MUST match `castStatus.ts` — verify against that file; the contract diff in Task 13 is the backstop.)* Add `status` (place before `start_datetime` to mirror the `fields` order at `config/export.ts:56`).

- [ ] **Step 2: Document the columns in the yml**

Add to `_export_partners.yml` under `columns:`:

```yaml
      - name: start_datetime_tz
        data_type: timestamp without time zone
      - name: start_datetime_utc
        data_type: timestamp without time zone
      - name: incentive_type
        data_type: text
      - name: status
        data_type: text
```

- [ ] **Step 3: Compile to verify the model parses**

Run: `cd datalake && just dbt compile --select export_partners`
Expected: compiles with no error; `target/compiled/.../export_partners.sql` contains the three new columns.

- [ ] **Step 4: Run the model and verify columns exist**

Run: `cd datalake && just dbt run --select export_partners`
Then verify (psql or `just dbt run-operation`):

```sql
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'zone_exposed' AND table_name = 'export_partners'
  AND column_name IN ('start_datetime_tz','start_datetime_utc','incentive_type');
```

Expected: 3 rows.

- [ ] **Step 5: Commit**

```bash
git add datalake/models/exposed/export/export_partners.sql datalake/models/exposed/export/yml/_export_partners.yml
git commit -S -m "feat(datalake): expose start_datetime_tz/utc + constant incentive_type on export_partners"
```

---

### Task 2: Index `trusted.carpools` for export filter columns

**Files:**
- Modify: `datalake/models/trusted/carpools.sql` (the `config()` `indexes` list, lines 1-18)

**Interfaces:**
- Produces: btree indexes on `operator_id`, `start_geo_code`, `end_geo_code` (the date index on `start_datetime_tz` already exists). Consumed implicitly by the worker query planner.

- [ ] **Step 1: Extend the `indexes` list**

Add to the existing `indexes = [ … ]` in `carpools.sql` (keep all existing entries):

```sql
      { 'columns':['operator_id'] },
      { 'columns':['start_geo_code'] },
      { 'columns':['end_geo_code'] },
```

(`start_datetime_tz` is already indexed — do not duplicate.)

- [ ] **Step 2: Full-refresh the model to build indexes**

Run: `cd datalake && just dbt run --select trusted.carpools --full-refresh`
Expected: model builds; run completes without error.

- [ ] **Step 3: Verify the indexes exist**

```sql
SELECT indexname FROM pg_indexes
WHERE schemaname='zone_trusted' AND tablename='carpools'
  AND indexdef ~ 'operator_id|start_geo_code|end_geo_code';
```

Expected: 3 rows.

- [ ] **Step 4: Verify a representative export range is index-driven**

Pick a settled month and run `EXPLAIN` for the shape the worker will issue (adjust dates):

```sql
EXPLAIN
SELECT journey_id FROM zone_exposed.export_partners
WHERE start_datetime_tz >= '2026-01-01' AND start_datetime_tz < '2026-02-01'
  AND operator_id IN (1);
```

Expected: an Index/Bitmap scan on `start_datetime_tz` (not a Seq Scan over all carpools). Record the plan in the commit message.

- [ ] **Step 5: Commit**

```bash
git add datalake/models/trusted/carpools.sql
git commit -S -m "perf(datalake): index carpools operator_id + geo codes for exports"
```

---

## Phase 2 — API control plane

### Task 3: Drop the unused `progress` mechanism

**Files:**
- Create: `api/src/db/migrations/20260707000000-drop-export-progress.sql`
- Modify: `api/src/pdc/services/export/repositories/ExportRepository.ts` (remove `progress()` impl lines 291-301, its resolver decl lines 113-124, the `ExportProgress` type line 16, and `progress` from `ExportUpdateData` line 14 and `UPDATABLE_COLUMNS` line 218)
- Modify: `api/src/pdc/services/export/models/Export.ts` (remove `progress` field line 27, line 43, line 61)
- Modify: `api/src/pdc/services/export/commands/ProcessCommand.ts` (remove the `this.exportRepository.progress(_id)` call, ~line 65)
- Modify: `api/src/pdc/services/export/services/FileCreatorService.ts` + `repositories/CarpoolRepository.ts` (remove `progress` params/plumbing)
- Modify: `api/src/pdc/services/export/actions/CreateAction.integration.spec.ts` (remove the `assertEquals(last?.progress, 0)` line)

**Interfaces:**
- Produces: `export.exports` no longer has a `progress` column; `ExportUpdateData` no longer accepts `progress`.

- [ ] **Step 1: Write the migration**

```sql
-- Drop the unused progress column from exports table
ALTER TABLE export.exports
DROP COLUMN progress;
```

- [ ] **Step 2: Remove `progress` from repository types + whitelist**

In `ExportRepository.ts`: delete the `ExportProgress` type (line 16); remove `"progress"` from the `ExportUpdateData` Pick (line 14); remove `"progress"` from `UPDATABLE_COLUMNS` (line 218); delete the `progress()` method (lines 291-301) and its abstract resolver declaration (lines 113-124).

- [ ] **Step 3: Remove `progress` from the model + callers**

In `Export.ts` delete the `progress` field and its two assignments. In `ProcessCommand.ts` remove the `progress` argument passed to file creation. In `FileCreatorService.ts` and `CarpoolRepository.ts` remove the `progress?: ExportProgress` params and the `if (progress) …` calls (row-count progress).

- [ ] **Step 4: Fix the test assertion**

In `CreateAction.integration.spec.ts`, delete the line `assertEquals(last?.progress, 0);`.

- [ ] **Step 5: Run migration + typecheck + export tests**

```bash
cd api
just migrate
deno check src/pdc/services/export/repositories/ExportRepository.ts
just test 'src/pdc/services/export/**/*.spec.ts'
```

Expected: migration applies; typecheck passes; export tests pass.

- [ ] **Step 6: Commit**

```bash
git add api/src/db/migrations/20260707000000-drop-export-progress.sql api/src/pdc/services/export
git commit -S -m "refactor(export): drop unused progress mechanism"
```

---

### Task 4: Add the `datalake.worker` group + `export.process` permission

**Files:**
- Modify: `api/src/pdc/services/auth/config/permissions.ts`
- Test: `api/src/pdc/services/auth/config/permissions.unit.spec.ts` (create if absent; follow existing `*.unit.spec.ts` style)

**Interfaces:**
- Produces: role `datalake.worker` whose permission set contains `datalake.export.process`; used by the 3 endpoints (Tasks 5-7) via `hasPermissionMiddleware("datalake.export.process")`.

- [ ] **Step 1: Write the failing test**

```typescript
import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { getPermissions } from "./permissions.ts";

describe("permissions", () => {
  it("grants export.process to datalake.worker", () => {
    assertEquals(getPermissions("datalake.worker").includes("datalake.export.process"), true);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd api && just test src/pdc/services/auth/config/permissions.unit.spec.ts`
Expected: FAIL — `datalake.worker` key missing (or throws in `dispatchPermissionsFromMatrix`).

- [ ] **Step 3: Add the group seed key + matrix entry**

In `dispatchPermissionsFromMatrix`, add to `permissionsByGroup`:

```typescript
    "datalake.worker": [],
```

In the `permissions` matrix (export block, near line 132), add:

```typescript
  "export.process": ["datalake.worker"],
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd api && just test src/pdc/services/auth/config/permissions.unit.spec.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/src/pdc/services/auth/config/permissions.ts api/src/pdc/services/auth/config/permissions.unit.spec.ts
git commit -S -m "feat(auth): add datalake.worker role with export.process permission"
```

---

### Task 4B: Remove the recipients feature (keep creator delivery)

**Files:**
- Delete: `api/src/pdc/services/export/repositories/RecipientRepository.ts`, `services/RecipientService.ts`, `models/ExportRecipient.ts`, `models/ExportRecipient.unit.spec.ts`
- Create: `api/src/db/migrations/20260707000100-drop-export-recipients.sql`
- Modify: `services/NotificationService.ts` (resolve creator instead of recipients), `repositories/ExportRepository.ts` (drop recipient coupling from `create()`/`ExportCreateData`, delete `recipients()`/`addRecipient()`/`recipientsTable`), `actions/CreateAction.ts`, `commands/CreateCommand.ts`, `contracts/create.contract.ts`, `contracts/create.schema.ts`, `ExportServiceProvider.ts`
- Test: `services/NotificationService.integration.spec.ts` (new), `actions/CreateAction.integration.spec.ts` (rewrite recipient assertions)

**Interfaces:**
- Produces: delivery now emails the export's **creator** (looked up from `created_by`); no recipients table, no extra-recipient input. `NotificationService.success(exp)` / `.error(exp)` resolve `userRepository.find(exp.created_by)` internally and email `"{fullname} <{email}>"`; `.support(exp)` unchanged. Consumed by Tasks 6 & 7 (complete/fail).

- [ ] **Step 1: Write the failing NotificationService test**

Assert `success(exp)` emails the creator resolved from `UserRepository`, with a mocked mailer + a mocked `UserRepository.find` returning `{ email: "u@test.fr", fullname: "U Ser" }`:

```typescript
it("emails the export creator on success", async () => {
  // arrange: exp.created_by = 42; UserRepository.find(42) -> { email:"u@test.fr", fullname:"U Ser" }
  // act: await notification.success(exp);
  // assert: mailer sent one mail to "u@test.fr" (or "U Ser <u@test.fr>")
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd api && just test src/pdc/services/export/services/NotificationService.integration.spec.ts`
Expected: FAIL (still reads `export.recipients`).

- [ ] **Step 3: Rework `NotificationService` to resolve the creator**

Inject `UserRepositoryInterfaceResolver`; replace the protected `recipients(exp)` helper (which read `exportRepository.recipients`) with:

```typescript
  protected async creator(exp: Export): Promise<{ email: string; fullname: string }> {
    const user = await this.userRepository.find(exp.created_by);
    return { email: user.email, fullname: user.fullname };
  }
```

Change `success(exp)` and `error(exp)` to email `this.creator(exp)` (single address). Remove the `ExportRecipient` import. Update the `NotificationService.success(...)` signature to take only `(exp, url)` (creator resolved internally). `support(exp)` is unchanged.

- [ ] **Step 4: Strip recipient coupling from `ExportRepository`**

Remove `recipients` from `ExportCreateData` (line 12); delete the recipient loop in `create()` (lines 195-198) so it just inserts + logs + returns; delete `recipients()` (iface 142-144, impl 313-321), `addRecipient()` (iface 154-159, impl 323-340), the `recipientsTable` property (177), and the `ExportRecipient` import (7).

- [ ] **Step 5: Strip recipients from the create paths**

In `CreateAction.ts`: remove `recipientService` injection, `maybeAddCreator`, the "no recipient" `InvalidParamsException`, `recipients` from the `create()` args, `"recipients"` from `castToArrayMiddleware`, and the two imports. In `CreateCommand.ts`: remove the `--recipient` option, `recipient` from `Options`, `maybeAddCreator`, `recipients` from `create()`, imports. In `create.contract.ts`: remove `recipients?: string[]`. In `create.schema.ts`: remove the `recipients` property. In `ExportServiceProvider.ts`: drop `RecipientService` + `RecipientRepository` registrations and imports.

- [ ] **Step 6: Delete the recipient files + write the drop migration**

`20260707000100-drop-export-recipients.sql`:

```sql
-- Remove the unused multi-recipient feature; exports now email the creator only.
DROP TABLE export.recipients;
```

Delete `RecipientRepository.ts`, `RecipientService.ts`, `ExportRecipient.ts`, `ExportRecipient.unit.spec.ts`.

- [ ] **Step 7: Fix the CreateAction integration test**

In `CreateAction.integration.spec.ts`: drop the `JOIN export.recipients` from `fetcher`, remove the "multiple recipients" test (line ~163), and turn the "fallback to created_by" test into the default single-creator expectation (no recipients rows asserted). Keep the create-succeeds assertions.

- [ ] **Step 8: Migrate, typecheck, run export tests**

```bash
cd api
just migrate
deno check src/pdc/services/export/services/NotificationService.ts
just test 'src/pdc/services/export/**/*.spec.ts'
```
Expected: migration applies; typecheck passes; tests pass (creator gets the email; no recipients table).

- [ ] **Step 9: Commit**

```bash
git add -A api/src/pdc/services/export api/src/db/migrations/20260707000100-drop-export-recipients.sql
git commit -S -m "refactor(export): remove recipients feature, notify export creator directly"
```

---

### Task 5: `POST /exports/claim` — atomic claim endpoint

**Files:**
- Create: `api/src/pdc/services/export/contracts/claim.contract.ts`
- Create: `api/src/pdc/services/export/contracts/claim.schema.ts`
- Create: `api/src/pdc/services/export/actions/ClaimAction.ts`
- Modify: `api/src/pdc/services/export/repositories/ExportRepository.ts` (add `claim(targets)` + resolver decl)
- Modify: `api/src/pdc/services/export/ExportServiceProvider.ts` (register action + validator)
- Test: `api/src/pdc/services/export/actions/ClaimAction.integration.spec.ts`

**Interfaces:**
- Consumes: `ExportStatus`, `ExportParams`, `hasPermissionMiddleware`.
- Produces: `claim(targets: string[]): Promise<Export | null>` on `ExportRepository`; endpoint returns `{ uuid, target, params }` (HTTP 200) or empty (HTTP 204).

- [ ] **Step 1: Add the `claim` method to the repository**

In `ExportRepository.ts`, add (mirrors `pickPending` but atomic + target-filtered, with `FOR UPDATE SKIP LOCKED` for safe concurrency):

```typescript
  public async claim(targets: string[]): Promise<Export | null> {
    const rows = await this.connection.query(sql`
      UPDATE ${raw(this.exportsTable)}
      SET status = ${ExportStatus.RUNNING}
      WHERE _id = (
        SELECT _id FROM ${raw(this.exportsTable)}
        WHERE status = 'pending'
          AND target = ANY(${targets})
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
  }
```

Add its declaration to `ExportRepositoryInterfaceResolver` (throwing `"Not implemented"`).

- [ ] **Step 2: Write the contract + schema**

`claim.contract.ts`:

```typescript
export type ParamsInterface = { targets: string[] };
export type ResultInterface =
  | { uuid: string; target: string; params: unknown }
  | null;
export const handlerConfig = { service: "exports", method: "claim" } as const;
export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
```

`claim.schema.ts` (follow `list.schema.ts` shape — a JSON-schema `binding` + exported `alias`):

```typescript
export const alias = "exports.claim";
export const schema = {
  $id: alias,
  type: "object",
  additionalProperties: false,
  required: ["targets"],
  properties: {
    targets: { type: "array", items: { type: "string", enum: ["operator", "territory"] }, minItems: 1 },
  },
};
export const binding = [alias, schema];
```

- [ ] **Step 3: Write the action**

`ClaimAction.ts`:

```typescript
import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/claim.contract.ts";
import { alias } from "../contracts/claim.schema.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("datalake.export.process"),
    ["validate", alias],
  ],
  apiRoute: { path: "/exports/claim", method: "POST", successHttpCode: 200 },
})
export class ClaimAction extends AbstractAction {
  constructor(protected exportRepository: ExportRepositoryInterfaceResolver) {
    super();
  }

  protected override async handle(
    params: ParamsInterface,
    _context: ContextType,
  ): Promise<ResultInterface> {
    // Opportunistic reaper: fail exports stuck 'running' past staleDelay.
    // This replaces ProcessCommand as the reaper caller (ProcessCommand is deleted in Task 15).
    await this.exportRepository.failStaleExports();

    const exp = await this.exportRepository.claim(params.targets);
    if (!exp) return null;
    return { uuid: exp.uuid, target: exp.target, params: exp.params.get() };
  }
}
```

> **Reaper semantics:** `failStaleExports()` sets stale `running` rows → `failure` (current behavior, preserved). A worker that dies mid-export therefore fails after `staleDelay` (default 4h) and the user re-requests. Ensure `staleDelay` stays safely above the max real export duration. *(Alternative self-heal — reset → `pending` for auto-retry — is deferred; it risks looping a poison export.)*

- [ ] **Step 4: Register in the ServiceProvider**

In `ExportServiceProvider.ts`: import `ClaimAction` and `binding as claimBinding`, add `ClaimAction` to `handlers`, add `claimBinding` to `validators`.

- [ ] **Step 5: Write the failing integration test**

`ClaimAction.integration.spec.ts` (mirror `CreateAction.integration.spec.ts` setup; seed two pending exports, claim, assert one flips to `running` and is returned):

```typescript
  const workerContext: ContextType = {
    call: { user: { permissions: ["datalake.export.process"] } },
    channel: { service: "proxy" },
  };

  it("claims the oldest pending export of the given target and marks it running", async () => {
    // arrange: insert a pending 'operator' export via the repo/SQL, capture its uuid
    await assertHandler(kc, workerContext, handlerConfig, { targets: ["operator"] },
      async (response: ResultInterface) => {
        assertEquals(response !== null, true);
        assertEquals(response!.target, "operator");
        // assert DB row is now 'running'
        const row = (await fetchByUuid(response!.uuid));
        assertEquals(row.status, "running");
      });
  });

  it("returns null when no pending export matches", async () => {
    await assertHandler(kc, workerContext, handlerConfig, { targets: ["territory"] },
      async (response: ResultInterface) => assertEquals(response, null));
  });
```

- [ ] **Step 6: Run the test**

Run: `cd api && just test src/pdc/services/export/actions/ClaimAction.integration.spec.ts`
Expected: both cases PASS.

- [ ] **Step 7: Commit**

```bash
git add api/src/pdc/services/export
git commit -S -m "feat(export): add POST /exports/claim atomic claim endpoint"
```

---

### Task 6: `POST /exports/:uuid/complete` — mark success + email (idempotent)

**Files:**
- Create: `api/src/pdc/services/export/contracts/complete.contract.ts`, `complete.schema.ts`
- Create: `api/src/pdc/services/export/actions/CompleteAction.ts`
- Modify: `api/src/pdc/services/export/ExportServiceProvider.ts`
- Test: `api/src/pdc/services/export/actions/CompleteAction.integration.spec.ts`

**Interfaces:**
- Consumes: `ExportRepository.get(uuid)`, `.update()`, `.status()`; `NotificationService.success(exp, url)` (creator-based after Task 4B); `hasPermissionMiddleware`.
- Produces: endpoint sets `filename = "{uuid}.csv.zip"`, `file_size`, `status = success`, and emails the creator — **exactly once** (guarded on the `running → success` transition).
- Depends on: Task 4B (creator-based `NotificationService`).

- [ ] **Step 1: Write the contract + schema**

`complete.contract.ts`:

```typescript
export type ParamsInterface = { uuid: string; file_size: number };
export type ResultInterface = { status: string };
export const handlerConfig = { service: "exports", method: "complete" } as const;
export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
```

`complete.schema.ts` — `$id: "exports.complete"`, required `uuid` (string, format uuid) + `file_size` (integer, min 0). `binding = [alias, schema]`.

- [ ] **Step 2: Write the action (idempotent success + email)**

```typescript
@handler({
  ...handlerConfig,
  middlewares: [hasPermissionMiddleware("datalake.export.process"), ["validate", alias]],
  apiRoute: { path: "/exports/:uuid/complete", method: "POST" },
})
export class CompleteAction extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected notification: NotificationServiceInterfaceResolver,
    protected recipientRepository: RecipientRepositoryInterfaceResolver,
  ) { super(); }

  protected override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const exp = await this.exportRepository.get(params.uuid);
    if (!exp) throw new NotFoundException(`Export ${params.uuid} not found`);
    if (exp.status === ExportStatus.SUCCESS) return { status: exp.status }; // idempotent no-op

    await this.exportRepository.update(exp._id, {
      filename: `${exp.uuid}.csv.zip`,
      file_size: params.file_size,
    });
    await this.exportRepository.status(exp._id, ExportStatus.SUCCESS);

    const recipients = await this.recipientRepository.findByExportId(exp._id);
    await this.notification.success(exp, recipients);

    return { status: ExportStatus.SUCCESS };
  }
}
```

*(Read `services/NotificationService.ts` + `repositories/RecipientRepository.ts` for the exact resolver names/signatures — mirror what `ProcessCommand` currently calls for the success email.)*

- [ ] **Step 3: Register in the ServiceProvider** (add action + validator binding).

- [ ] **Step 4: Write the failing integration test**

Assert: (a) a `running` export → `success`, `filename = {uuid}.csv.zip`, `file_size` stored, one email sent; (b) calling complete again returns `success` and does **not** send a second email (assert the mailer/notification spy called once).

- [ ] **Step 5: Run the test**

Run: `cd api && just test src/pdc/services/export/actions/CompleteAction.integration.spec.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/src/pdc/services/export
git commit -S -m "feat(export): add POST /exports/:uuid/complete (idempotent success + email)"
```

---

### Task 7: `POST /exports/:uuid/fail` — mark failure + error email

**Files:**
- Create: `api/src/pdc/services/export/contracts/fail.contract.ts`, `fail.schema.ts`
- Create: `api/src/pdc/services/export/actions/FailAction.ts`
- Modify: `api/src/pdc/services/export/ExportServiceProvider.ts`
- Test: `api/src/pdc/services/export/actions/FailAction.integration.spec.ts`

**Interfaces:**
- Consumes: `ExportRepository.get(uuid)`, `.error(id, message)`; the existing error-notification path used by `ProcessCommand`.
- Produces: endpoint sets `status = failure`, stores `error`, emails user + support (idempotent: no-op if already `failure`).

- [ ] **Step 1: Contract + schema** — `ParamsInterface = { uuid: string; message: string }`; schema `$id: "exports.fail"`, required `uuid` + `message` (string).

- [ ] **Step 2: Write the action**

```typescript
  protected override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const exp = await this.exportRepository.get(params.uuid);
    if (!exp) throw new NotFoundException(`Export ${params.uuid} not found`);
    if (exp.status === ExportStatus.FAILURE) return { status: exp.status };
    await this.exportRepository.error(exp._id, params.message);
    // mirror ProcessCommand's failure notification (user + support)
    await this.notification.failure(exp, params.message);
    return { status: ExportStatus.FAILURE };
  }
```

- [ ] **Step 3: Register in the ServiceProvider.**

- [ ] **Step 4: Write the failing integration test** — `running` → `failure`, `error` JSON stored, failure email sent; second call is a no-op.

- [ ] **Step 5: Run the test**

Run: `cd api && just test src/pdc/services/export/actions/FailAction.integration.spec.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/src/pdc/services/export
git commit -S -m "feat(export): add POST /exports/:uuid/fail (failure + error email)"
```

---

## Phase 3 — Datalake worker

### Task 8: Introduce pytest + an S3 upload helper for the `export` bucket

**Files:**
- Modify: `datalake/pyproject.toml` (add `pytest` + `requests` to the `dev` dependency group; `requests` is already used by `helpers/url.py` but confirm it's a declared dep)
- Modify: `datalake/pipelines/helpers/s3.py` (add `s3_upload` + an `export_s3_client`)
- Create: `datalake/pipelines/helpers/__init__.py` test dir: `datalake/tests/test_s3_helpers.py`

**Interfaces:**
- Produces: `s3_upload(bucket, key, local_path, client=None)` and `export_s3_client()` (reads `EXPORT_S3_ENDPOINT`/`EXPORT_S3_ACCESS_KEY`/`EXPORT_S3_SECRET_KEY` — the export bucket has different creds/lifecycle than the datalake bucket). Consumed by the worker (Task 11).

- [ ] **Step 1: Add pytest + psycopg**

In `pyproject.toml`: add `"psycopg[binary]>=3.2"` to `[project].dependencies` (state-of-the-art driver; streams `COPY … TO STDOUT` with no DuckDB overhead), and `"pytest>=8"` to `[dependency-groups].dev`. Run `cd datalake && uv sync` to update the lock.

Also add a Postgres conninfo helper to `datalake/pipelines/helpers/duckdb.py`'s sibling — create `datalake/pipelines/helpers/pg.py`:

```python
import os

def pg_conninfo() -> str:
    return (
        f"host={os.getenv('DBT_HOST')} port={os.getenv('DBT_PORT')} "
        f"user={os.getenv('DBT_USER')} password={os.getenv('DBT_PASSWORD')} "
        f"dbname={os.getenv('DBT_DBNAME')}"
    )
```

- [ ] **Step 2: Write the failing test**

`datalake/tests/test_s3_helpers.py`:

```python
from unittest.mock import MagicMock
from pipelines.helpers.s3 import s3_upload

def test_s3_upload_calls_put_with_key(tmp_path):
    f = tmp_path / "x.csv.zip"
    f.write_bytes(b"zipdata")
    client = MagicMock()
    s3_upload("export", "abc.csv.zip", str(f), client=client)
    client.upload_file.assert_called_once_with(str(f), "export", "abc.csv.zip")
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cd datalake && uv run pytest tests/test_s3_helpers.py -v`
Expected: FAIL — `s3_upload` not defined.

- [ ] **Step 4: Implement the helpers**

Add to `s3.py`:

```python
def export_s3_client():
    return s3_client(
        endpoint=os.getenv("EXPORT_S3_ENDPOINT"),
        access_key=os.getenv("EXPORT_S3_ACCESS_KEY"),
        secret_key=os.getenv("EXPORT_S3_SECRET_KEY"),
    )

def s3_upload(bucket: str, key: str, local_path: str, client=None) -> None:
    client = client or export_s3_client()
    client.upload_file(local_path, bucket, key)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd datalake && uv run pytest tests/test_s3_helpers.py -v`
Expected: PASS.

- [ ] **Step 6: Add env vars to `.env.example`**

```bash
# --- Export bucket (on-demand exports; different lifecycle/creds than S3_*) ---
EXPORT_S3_ENDPOINT=
EXPORT_S3_ACCESS_KEY=
EXPORT_S3_SECRET_KEY=
EXPORT_S3_BUCKET=
# --- API control plane ---
API_URL=
EXPORT_WORKER_ACCESS_KEY=
EXPORT_WORKER_SECRET_KEY=
```

- [ ] **Step 7: Commit**

```bash
git add datalake/pyproject.toml datalake/uv.lock datalake/pipelines/helpers/s3.py datalake/tests/test_s3_helpers.py datalake/.env.example
git commit -S -m "feat(datalake): pytest + export-bucket S3 upload helper"
```

---

### Task 9: Worker query builder — per-target field list + WHERE (pure functions)

**Files:**
- Create: `datalake/pipelines/helpers/export_query.py`
- Test: `datalake/tests/test_export_query.py`

**Interfaces:**
- Produces:
  - `FIELDS: list[str]` and `EXCLUSIONS: dict[str, list[str]]` (verbatim ports of `config/export.ts` `fields` + `filters`).
  - `select_columns(target: str) -> list[str]` → the ordered CSV columns for a target.
  - `geo_to_sql(geo_selector: dict | None, mode="OR") -> str` (port of `ExportParams.geoToSQL`).
  - `operator_to_sql(operator_id: list[int]) -> str` (port of `ExportParams.operatorToSQL`).
  - `build_copy_sql(target, params) -> str` → the full `COPY (SELECT … WHERE …) TO STDOUT`-style inner query string.
- Consumed by the worker (Task 11).

- [ ] **Step 1: Write the failing tests**

`datalake/tests/test_export_query.py`:

```python
from pipelines.helpers.export_query import (
    select_columns, geo_to_sql, operator_to_sql, build_copy_sql,
)

def test_operator_excludes_operator_and_has_incentive():
    cols = select_columns("operator")
    assert "operator" not in cols
    assert "has_incentive" not in cols
    assert "incentive_type" in cols
    assert cols[0] == "journey_id"

def test_territory_excludes_has_incentive_only():
    cols = select_columns("territory")
    assert "has_incentive" not in cols
    assert "operator" in cols

def test_operator_to_sql():
    assert operator_to_sql([1, 2]) == "AND operator_id IN (1,2)"
    assert operator_to_sql([]) == ""

def test_geo_to_sql_epci_maps_to_epci_code():
    out = geo_to_sql({"epci": ["200054781"]})
    assert "start_epci_code = '200054781'" in out
    assert "end_epci_code = '200054781'" in out
    assert out.startswith("AND ((")

def test_geo_to_sql_empty():
    assert geo_to_sql(None) == ""
    assert geo_to_sql({"epci": []}) == ""

def test_build_copy_sql_has_date_and_target_predicates():
    sql = build_copy_sql("operator", {
        "start_at": "2026-01-01", "end_at": "2026-02-01",
        "operator_id": [1], "geo_selector": None,
    })
    assert "start_datetime_tz >= '2026-01-01'" in sql
    assert "start_datetime_tz < '2026-02-01'" in sql
    assert "operator_id IN (1)" in sql
    assert "FROM zone_exposed.export_partners" in sql

# --- SQL-injection defense (params come from a user's export request) ---
import pytest

def test_geo_to_sql_rejects_injection_in_code():
    with pytest.raises(ValueError):
        geo_to_sql({"epci": ["200054781'; DROP TABLE carpools;--"]})

def test_geo_to_sql_rejects_unknown_key():
    with pytest.raises(ValueError):
        geo_to_sql({"evil": ["123"]})

def test_build_copy_sql_rejects_bad_date():
    with pytest.raises(ValueError):
        build_copy_sql("operator", {
            "start_at": "2026-01-01') TO STDOUT; --", "end_at": "2026-02-01",
            "operator_id": [1], "geo_selector": None,
        })

def test_operator_to_sql_coerces_ints():
    # non-int input must raise, not interpolate
    with pytest.raises(ValueError):
        operator_to_sql(["1; DROP TABLE"])
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd datalake && uv run pytest tests/test_export_query.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `export_query.py`**

Port `fields`/`filters` verbatim from `config/export.ts`, and `geoToSQL`/`operatorToSQL` from `ExportParams.ts` (with strict input validation — see the security block):

```python
import re

FIELDS = [
    "journey_id", "operator_trip_id", "operator_journey_id", "operator_class",
    "status", "start_datetime", "start_date", "start_time",
    "end_datetime", "end_date", "end_time", "duration", "distance",
    "start_lat", "start_lon", "end_lat", "end_lon",
    "start_insee", "start_commune", "start_departement", "start_epci",
    "start_aom", "start_region", "start_pays",
    "end_insee", "end_commune", "end_departement", "end_epci",
    "end_aom", "end_region", "end_pays",
    "operator", "operator_passenger_id", "passenger_identity_key",
    "operator_driver_id", "driver_identity_key",
    "driver_revenue", "passenger_contribution", "passenger_seats",
    "cee_application", "incentive_type", "has_incentive",
    "incentive_0_siret", "incentive_0_name", "incentive_0_amount",
    "incentive_1_siret", "incentive_1_name", "incentive_1_amount",
    "incentive_2_siret", "incentive_2_name", "incentive_2_amount",
    "incentive_rpc_0_campaign_id", "incentive_rpc_0_campaign_name",
    "incentive_rpc_0_siret", "incentive_rpc_0_name", "incentive_rpc_0_amount",
    "incentive_rpc_1_campaign_id", "incentive_rpc_1_campaign_name",
    "incentive_rpc_1_siret", "incentive_rpc_1_name", "incentive_rpc_1_amount",
    "incentive_rpc_2_campaign_id", "incentive_rpc_2_campaign_name",
    "incentive_rpc_2_siret", "incentive_rpc_2_name", "incentive_rpc_2_amount",
]

EXCLUSIONS = {
    "operator": ["operator", "has_incentive"],
    "territory": ["has_incentive"],
}

COLUMN_MAP = {"epci": "epci_code", "aom": "aom_code"}

# Security: these params originate from a user's export request (stored in
# export.exports.params) and are interpolated into SQL. Validate strictly here
# (defense-in-depth) even though the API validates upstream. See build_copy_sql.
ALLOWED_GEO_KEYS = {"arr", "com", "dep", "epci", "aom", "reg", "country", "insee"}
_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,15}$")   # INSEE/EPCI/AOM/region/country codes
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")    # worker normalizes to YYYY-MM-DD

def select_columns(target: str) -> list[str]:
    excluded = set(EXCLUSIONS.get(target, []))
    return [f for f in FIELDS if f not in excluded]

def operator_to_sql(operator_id: list[int]) -> str:
    if not operator_id:
        return ""
    joined = ",".join(str(int(o)) for o in operator_id)   # int() guards injection
    return f"AND operator_id IN ({joined})"

def geo_to_sql(geo_selector: dict | None, mode: str = "OR") -> str:
    if not geo_selector:
        return ""
    groups = []
    for key, codes in geo_selector.items():
        if not codes:
            continue
        if key not in ALLOWED_GEO_KEYS:                     # allowlist the identifier
            raise ValueError(f"invalid geo selector key: {key!r}")
        col = COLUMN_MAP.get(key, key)
        for c in codes:
            if not _CODE_RE.match(str(c)):                  # allowlist each value
                raise ValueError(f"invalid geo code: {c!r}")
        groups.append(" OR ".join(f"start_{col} = '{c}'" for c in codes))
    if not groups:
        return ""
    start = " OR ".join(groups)
    end = start.replace("start_", "end_")                   # safe: codes are [A-Za-z0-9]
    return f"AND (({start}) {mode} ({end}))"

def build_copy_sql(target: str, params: dict) -> str:
    cols = ", ".join(select_columns(target))
    start_at = str(params["start_at"])
    end_at = str(params["end_at"])
    if not _DATE_RE.match(start_at) or not _DATE_RE.match(end_at):
        raise ValueError(f"invalid date bounds: {start_at!r}..{end_at!r}")
    geo = geo_to_sql(params.get("geo_selector"))
    op = operator_to_sql(params.get("operator_id", []))
    return f"""
        SELECT {cols}
        FROM zone_exposed.export_partners
        WHERE start_datetime_tz >= '{start_at}'
          AND start_datetime_tz < '{end_at}'
          {geo}
          {op}
        ORDER BY start_datetime_tz ASC
    """
```

*(Note: `start_at`/`end_at` arrive from the API as ISO datetimes; the worker (Task 11) normalizes them to `YYYY-MM-DD` local dates before calling — matching today's `start_date_filter` day-boundary semantics.)*

- [ ] **Step 4: Run to verify pass**

Run: `cd datalake && uv run pytest tests/test_export_query.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add datalake/pipelines/helpers/export_query.py datalake/tests/test_export_query.py
git commit -S -m "feat(datalake): export query builder (per-target fields + geo/operator WHERE)"
```

---

### Task 10: API HTTP client for the worker (claim/complete/fail + auth)

**Files:**
- Create: `datalake/pipelines/helpers/api_client.py`
- Test: `datalake/tests/test_api_client.py`

**Interfaces:**
- Produces: `ApiClient(base_url, access_key, secret_key)` with `.claim(targets) -> dict | None`, `.complete(uuid, file_size) -> None`, `.fail(uuid, message) -> None`. Fetches a Bearer token via `POST /auth/access_token` and retries on 5xx/connection errors with backoff. Consumed by the worker (Task 11).

- [ ] **Step 1: Write the failing tests** (mock `requests`):

```python
from unittest.mock import MagicMock, patch
from pipelines.helpers.api_client import ApiClient

def _client():
    return ApiClient("https://api.test", "ak", "sk")

@patch("pipelines.helpers.api_client.requests")
def test_claim_returns_none_on_204(rq):
    rq.post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"access_token": "t"}),  # token
        MagicMock(status_code=204),                                       # claim
    ]
    assert _client().claim(["operator"]) is None

@patch("pipelines.helpers.api_client.requests")
def test_claim_returns_payload_on_200(rq):
    rq.post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"access_token": "t"}),
        MagicMock(status_code=200, json=lambda: {"uuid": "u", "target": "operator", "params": {}}),
    ]
    assert _client().claim(["operator"])["uuid"] == "u"
```

- [ ] **Step 2: Run to verify fail**

Run: `cd datalake && uv run pytest tests/test_api_client.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `api_client.py`** (token exchange mirrors `POST /auth/access_token` with `access_key`/`secret_key`; the RPC actions return their result under the proxy's JSON envelope — confirm the exact response shape against `registerExpressRoute` and unwrap accordingly):

```python
import time
import requests

class ApiClient:
    def __init__(self, base_url, access_key, secret_key):
        self.base_url = base_url.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self._token = None

    def _auth(self):
        if self._token:
            return self._token
        r = requests.post(f"{self.base_url}/auth/access_token",
                          json={"access_key": self.access_key, "secret_key": self.secret_key},
                          timeout=30)
        r.raise_for_status()
        self._token = r.json()["access_token"]
        return self._token

    def _headers(self):
        return {"Authorization": f"Bearer {self._auth()}"}

    def _post(self, path, body, retries=3):
        for attempt in range(retries):
            try:
                r = requests.post(f"{self.base_url}{path}", json=body,
                                  headers=self._headers(), timeout=60)
                if r.status_code >= 500:
                    raise requests.RequestException(f"{r.status_code}")
                return r
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def claim(self, targets):
        r = self._post("/exports/claim", {"targets": targets})
        if r.status_code == 204:
            return None
        return r.json()

    def complete(self, uuid, file_size):
        self._post(f"/exports/{uuid}/complete", {"uuid": uuid, "file_size": file_size})

    def fail(self, uuid, message):
        self._post(f"/exports/{uuid}/fail", {"uuid": uuid, "message": message})
```

- [ ] **Step 4: Run to verify pass**

Run: `cd datalake && uv run pytest tests/test_api_client.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datalake/pipelines/helpers/api_client.py datalake/tests/test_api_client.py
git commit -S -m "feat(datalake): API client for export control plane"
```

---

### Task 11: The worker command (claim → COPY → zip → upload → complete/fail)

**Files:**
- Create: `datalake/pipelines/cmd/export_worker.py`
- Modify: `datalake/justfile` (add `export-worker` recipe)
- Test: `datalake/tests/test_export_worker.py`

**Interfaces:**
- Consumes: `ApiClient` (Task 10), `build_copy_sql` (Task 9), `pg_conninfo` (Task 8), `psycopg`, `s3_upload`/`EXPORT_S3_BUCKET` (Task 8).
- Produces: `stream_csv(conn, inner_sql, csv_path)` (psycopg `COPY … TO STDOUT` → file), `process_one(api, conn, s3, bucket) -> bool` (True if it processed an export, False if the queue was empty), and a Typer `run` command that loop-drains until empty.

- [ ] **Step 1: Write the failing test** (mock api + a fake psycopg conn + s3):

```python
from unittest.mock import MagicMock, patch
from pipelines.cmd.export_worker import process_one

def test_process_one_empty_returns_false():
    api = MagicMock(); api.claim.return_value = None
    assert process_one(api, MagicMock(), MagicMock(), "export") is False

@patch("pipelines.cmd.export_worker.stream_csv")
def test_process_one_success_flow(stream_csv, tmp_path, monkeypatch):
    # stream_csv writes a small CSV so zip + size work
    def _fake_stream(conn, inner, csv_path):
        with open(csv_path, "w") as f:
            f.write("journey_id\n1\n")
    stream_csv.side_effect = _fake_stream

    api = MagicMock()
    api.claim.return_value = {"uuid": "u1", "target": "operator",
                              "params": {"start_at": "2026-01-01T00:00:00+0100",
                                         "end_at": "2026-02-01T00:00:00+0100",
                                         "operator_id": [1], "geo_selector": None}}
    conn = MagicMock()
    s3 = MagicMock()
    monkeypatch.chdir(tmp_path)
    ok = process_one(api, conn, s3, "export")
    assert ok is True
    stream_csv.assert_called_once()
    s3.upload_file.assert_called_once()
    assert s3.upload_file.call_args[0][2] == "u1.csv.zip"
    api.complete.assert_called_once()
    assert api.complete.call_args[0][0] == "u1"

@patch("pipelines.cmd.export_worker.stream_csv", side_effect=RuntimeError("boom"))
def test_process_one_failure_calls_fail(stream_csv, tmp_path, monkeypatch):
    api = MagicMock()
    api.claim.return_value = {"uuid": "u2", "target": "operator",
                              "params": {"start_at": "2026-01-01T00:00:00+0100",
                                         "end_at": "2026-02-01T00:00:00+0100",
                                         "operator_id": [1], "geo_selector": None}}
    monkeypatch.chdir(tmp_path)
    assert process_one(api, MagicMock(), MagicMock(), "export") is True
    api.fail.assert_called_once()
    assert api.fail.call_args[0][0] == "u2"
```

- [ ] **Step 2: Run to verify fail**

Run: `cd datalake && uv run pytest tests/test_export_worker.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `export_worker.py`** (psycopg `COPY … TO STDOUT` streamed to file — no DuckDB):

```python
import os
import zipfile
import typer
import psycopg
from datetime import datetime
from dotenv import load_dotenv
from pipelines.helpers.pg import pg_conninfo
from pipelines.helpers.s3 import export_s3_client, s3_upload
from pipelines.helpers.api_client import ApiClient
from pipelines.helpers.export_query import build_copy_sql

load_dotenv()
app = typer.Typer()

TARGETS = ["operator", "territory"]


def _to_local_date(iso: str) -> str:
    # normalize ISO datetime to YYYY-MM-DD (day-boundary semantics like start_date_filter)
    return datetime.fromisoformat(iso).strftime("%Y-%m-%d")


def stream_csv(conn, inner_sql: str, csv_path: str) -> None:
    # server-side streaming COPY straight to a local file, semicolon-delimited + header
    copy_sql = f"COPY ({inner_sql}) TO STDOUT (FORMAT CSV, DELIMITER ';', HEADER)"
    with open(csv_path, "wb") as f, conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for chunk in copy:
                f.write(chunk)


def process_one(api, conn, s3, bucket) -> bool:
    task = api.claim(TARGETS)
    if not task:
        return False
    uuid = task["uuid"]
    try:
        params = dict(task["params"])
        params["start_at"] = _to_local_date(params["start_at"])
        params["end_at"] = _to_local_date(params["end_at"])
        inner = build_copy_sql(task["target"], params)

        csv_path = f"./{uuid}.csv"
        zip_path = f"./{uuid}.csv.zip"
        stream_csv(conn, inner, csv_path)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(csv_path, arcname=f"{uuid}.csv")

        s3_upload(bucket, f"{uuid}.csv.zip", zip_path, client=s3)
        api.complete(uuid, os.path.getsize(zip_path))
        return True
    except Exception as e:  # any failure → report to API, don't crash the loop
        api.fail(uuid, str(e))
        return True
    finally:
        for p in (f"./{uuid}.csv", f"./{uuid}.csv.zip"):
            if os.path.exists(p):
                os.unlink(p)


@app.command()
def run(max_iterations: int = 50):
    api = ApiClient(os.environ["API_URL"],
                    os.environ["EXPORT_WORKER_ACCESS_KEY"],
                    os.environ["EXPORT_WORKER_SECRET_KEY"])
    s3 = export_s3_client()
    bucket = os.environ["EXPORT_S3_BUCKET"]
    processed = 0
    with psycopg.connect(pg_conninfo()) as conn:
        while processed < max_iterations and process_one(api, conn, s3, bucket):
            processed += 1
    print(f"✅ processed {processed} export(s)")


if __name__ == "__main__":
    app()
```

- [ ] **Step 4: Run to verify pass**

Run: `cd datalake && uv run pytest tests/test_export_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Add the justfile recipe**

```makefile
# Drain the on-demand export queue (claims from API, writes to export bucket)
export-worker *args:
  python -m pipelines.cmd.export_worker {{args}}
```

- [ ] **Step 6: Commit**

```bash
git add datalake/pipelines/cmd/export_worker.py datalake/justfile datalake/tests/test_export_worker.py
git commit -S -m "feat(datalake): on-demand export worker command"
```

---

## Phase 4 — Validation, cutover, cleanup

### Task 12: End-to-end dry run on staging

**Files:** none (operational).

- [ ] **Step 1: Provision the worker credential** — issue a Dex `datalake.worker` application credential (`operator_id = 0`) and set `EXPORT_WORKER_ACCESS_KEY`/`SECRET_KEY`, `API_URL`, `EXPORT_S3_*` in the datalake staging env. *(Infra owner does the secrets.)*

- [ ] **Step 2: Create a test export** on staging via `POST /exports` (target `operator`, a settled month, a recipient you control).

- [ ] **Step 3: Run the worker once**

Run: `cd datalake && just export-worker run`
Expected: `processed 1 export(s)`; object `{uuid}.csv.zip` present in the `export` bucket.

- [ ] **Step 4: Verify delivery** — `POST /exports/list` shows `status=success`, `filename={uuid}.csv.zip`, `file_size>0`; `GET /exports/download-link/:id` returns a working presigned URL; the recipient email arrived.

- [ ] **Step 5: Verify the failure path** — temporarily point `build_copy_sql` at a bad column (or claim then kill mid-run), confirm `POST /exports/:uuid/fail` fires, row goes `failure`, error email sent, and a later worker run does not double-process.

---

### Task 13: Contract parity diff on a settled range (BLOCKING before flip)

**Files:**
- Create: `datalake/tests/contract/diff_export.md` (a short runbook capturing the procedure + result)

**Interfaces:** none — this is a validation gate.

- [ ] **Step 1: Pick a fully-settled month** (≥ 2 months old, so trusted == live) and an `operator` target with a known `operator_id`.

- [ ] **Step 2: Produce the OLD output** — run the current API path for that request (`just api export:process` against a seeded pending row, or call `carpoolListQuery` directly) and capture the CSV.

- [ ] **Step 3: Produce the NEW output** — run the worker for the same params; download `{uuid}.csv.zip` and unzip.

- [ ] **Step 4: Diff** — compare header (column names + order) and body:

```bash
unzip -p old.csv.zip | sort > /tmp/old.sorted
unzip -p new.csv.zip | sort > /tmp/new.sorted
head -1 <(unzip -p old.csv.zip); head -1 <(unzip -p new.csv.zip)   # header order must match exactly
diff /tmp/old.sorted /tmp/new.sorted | head -50
```

Expected: identical headers; zero body diff. **Confirm empirically that `incentive_type` is `'normal'` on every row of the OLD output** (validates the constant). Record the outcome in `diff_export.md`. Any diff blocks the flip — fix `export_partners`/`export_query.py` and repeat. Repeat once for a `territory` target.

- [ ] **Step 5: Commit the runbook**

```bash
git add datalake/tests/contract/diff_export.md
git commit -S -m "test(datalake): export contract parity runbook + settled-range result"
```

---

### Task 14: Strangler — target exclusion on the API worker

**Files:**
- Modify: `api/src/pdc/services/export/repositories/ExportRepository.ts` (`pickPending` — exclude datalake-owned targets)
- Modify: `api/src/pdc/services/export/config/export.ts` (add `excludeTargets` env-backed config)
- Test: `api/src/pdc/services/export/repositories/ExportRepository.integration.spec.ts` (or extend existing)

**Interfaces:**
- Produces: `pickPending()` skips targets listed in `APP_EXPORT_EXCLUDE_TARGETS` so the API and datalake workers never claim the same row during rollout.

- [ ] **Step 1: Add config**

In `config/export.ts`:

```typescript
export const excludeTargets = (Deno.env.get("APP_EXPORT_EXCLUDE_TARGETS") ?? "")
  .split(",").map((s) => s.trim()).filter(Boolean);
```

- [ ] **Step 2: Write the failing test** — with `excludeTargets=['operator']`, seed one pending `operator` + one pending `territory`; assert `pickPending()` returns the `territory` row and never the `operator` one.

- [ ] **Step 3: Update `pickPending`**

```typescript
  public async pickPending(): Promise<Export | null> {
    const rows = await this.connection.query(sql`
        SELECT * FROM ${raw(this.exportsTable)}
        WHERE status = 'pending'
          AND target <> ALL(${excludeTargets})
        ORDER BY created_at ASC
        LIMIT 1
      `);
    return rows.length ? Export.fromJSON(rows[0]) : null;
  }
```

(import `excludeTargets` from `../config/export.ts`.)

- [ ] **Step 4: Run the test**

Run: `cd api && just test src/pdc/services/export/repositories/ExportRepository.integration.spec.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/src/pdc/services/export
git commit -S -m "feat(export): exclude datalake-owned targets from API worker queue"
```

- [ ] **Step 6: Rollout (operational)** — set `APP_EXPORT_EXCLUDE_TARGETS=operator` on the API and deploy the datalake worker CronJob with `TARGETS`-effective `operator`. Watch both for a few days. Then add `territory` to both. Then the API worker owns nothing.

---

### Task 15: Delete API file-production + retire `export_carpool_list`

**Files:**
- Delete: `api/src/pdc/services/export/commands/ProcessCommand.ts`, `services/FileCreatorService.ts`, `models/CSVWriter.ts`, `models/CarpoolRow.ts`, `models/Campaign.ts`, `repositories/CarpoolRepository.ts`, `repositories/queries/carpoolListQuery.ts`, `repositories/queries/carpoolCountQuery.ts`, `repositories/CampaignRepository.ts`
- Modify: `api/src/pdc/services/export/ExportServiceProvider.ts` (drop the deleted registrations)
- Modify: `sqlmesh/models/3_refined_zone/export/export_carpool_list.sql` — **delete** (keep `export_opendata_list.sql`, still used by `datagouvListQuery.ts`)

**Interfaces:** none produced — this is removal. Only do this after Task 14's flip is complete and stable.

- [ ] **Step 1: Confirm nothing else references the deleted symbols**

Run:
```bash
cd api && grep -rn "ProcessCommand\|FileCreatorService\|CSVWriter\|CarpoolRepository\|carpoolListQuery\|export_carpool_list" src --include="*.ts" | grep -v "\.spec\.ts"
```
Expected: only the files being deleted + `ExportServiceProvider.ts` registrations.

- [ ] **Step 2: Delete the files and their registrations/imports** in `ExportServiceProvider.ts` (remove from `services`, `repositories`, `commands`, and imports). Delete `export_carpool_list.sql`.

- [ ] **Step 3: Delete their tests** (`*.spec.ts` for the removed modules).

- [ ] **Step 4: Typecheck + run remaining export tests**

```bash
cd api && deno check src/pdc/services/export/ExportServiceProvider.ts
just test 'src/pdc/services/export/**/*.spec.ts'
```
Expected: passes with the reduced surface.

- [ ] **Step 5: Verify sqlmesh still parses without the carpool view**

Run: `cd sqlmesh && sqlmesh plan --no-prompts` (dev) or `sqlmesh diff` — confirm `export_opendata_list` is untouched and only `export_carpool_list` is removed.

- [ ] **Step 6: Commit**

```bash
git add -A api/src/pdc/services/export sqlmesh/models/3_refined_zone/export
git commit -S -m "chore(export): remove API file production, retire export_carpool_list (datalake owns exports)"
```

---

## Notes / follow-ups (out of scope here)

- **Human-readable download filename.** The worker uses the deterministic key `{uuid}.csv.zip`, so the user-facing download name is the UUID rather than today's `NameService` name. If preserving the human name matters, add a follow-up to set S3 `Content-Disposition` on upload or store a display name. (Deferred.)
- **Monthly data.gouv.fr publication** — separate later spec (keeps `export_opendata_list`).
- **`download_url` column** stays (UI reads it); revisit as post-cutover cleanup.
- **`incentive_type` real logic** — deferred data-plumbing project (surface `booster_dates` as data).
