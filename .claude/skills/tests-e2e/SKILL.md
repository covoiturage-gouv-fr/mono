---
name: tests-e2e
description: >
  Use when running, debugging, or reasoning about the API's integration and e2e
  test stack (the `(api) integration tests` / `(api) e2e tests` CI jobs, the
  "Build stack" step, `just ci_test_*` / `just dc_e2e` recipes, or `*.integration.spec.ts`
  / e2e specs). Triggers whenever CI hangs or fails on building the Docker test
  stack, when an integration spec needs to run locally (there is no bare local
  Postgres — the DB only exists inside the compose stack), or when diagnosing the
  `wait` service, a stuck "Build stack", or a Postgres/api container that won't come up.
  Also use before telling someone "just run the integration tests locally" — the
  stack has real gotchas (busybox DNS, stale volumes, required env) documented here.
---

# API integration & e2e test stack

The backend integration/e2e tests run against a **docker-compose stack**, not a
bare local database. Everything is driven from `api/` via `just` recipes. If you
try to run `*.integration.spec.ts` without the stack, it fails — the DB lives only
in the stack.

## Stack shape

`just dc_e2e <args>` = `docker compose` with three overlaid files (run from `api/`):

```
docker/docker-compose.base.yml   # postgres, redis, mailer, s3, dex, api, one-off: wait, s3-init
docker/docker-compose.proxy.yml  # proxy (traefik)
docker/docker-compose.e2e.yml    # e2e overrides
```

Services: **postgres** (5432), **redis** (6379), **mailer** (mailpit 1025/8025),
**s3** (minio 9000), **dex** (OIDC), **proxy** (traefik 80/443), **api** (8080,
built from `api/Dockerfile`). Compose project name is `rpc`, so containers are
`rpc-postgres-1` etc. and the shared network is `rpc_back`.

## How CI builds the stack (`.github/actions/build-stack` ≈ `just ci_test_prehook`)

The composite action `./.github/actions/build-stack` runs, in order:

1. `just generate_certs` + `just generate_keys` — local TLS/JWT material (no docker).
2. `just dc_e2e up -d postgres redis s3 mailer dex` — start base services.
3. `just wait postgres 5432` / `redis 6379` / `s3 9000` — **block until each is reachable**.
4. `just dc_e2e run api just cache` / `cache-migrator` / `seed` — cache deps, run
   migrations, seed test data (this is where a new migration first executes).
5. `just dc_e2e up -d proxy api` — build + start the api image and traefik.
6. `just create_bucket ...` ×3 — minio buckets.
7. `just wait api 8080` — block until the api answers.

Then the test job runs, inside the api container:
`just dc_e2e exec api just test-integration` (or `test-e2e`) — i.e.
`deno test --no-check -A ... '**/*.integration.spec.ts'`.

Recipes worth knowing: `just ci_test_prehook` (steps 1-7), `just ci_test_integration`
(prehook → tests → posthook), `just ci_test_e2e`, `just ci_test_posthook`
(`dc_e2e down -v` teardown).

## The `wait` service is the #1 hang point

Step 3 uses a **one-off busybox container** with no timeout:

```yaml
wait:
  image: busybox:1.37   # was 1.33 — see below
  command: ["/bin/sh","-c","until nc -zv $$DOMAIN $$PORT -w1; do echo Waiting; sleep 1; done"]
  networks: [back]
```

If `nc` can't resolve the service name, it prints `nc: bad address 'postgres'` and
the `until` loop **spins forever** — nothing times it out, so the "Build stack" step
hangs indefinitely (seen as a ~1h+ stuck job, then cancelled). Nothing after it runs,
so the tests never start.

**Known root cause — busybox 1.33 vs modern Docker DNS.** busybox `1.33`'s musl
resolver fails against recent Docker's embedded DNS (127.0.0.11). Full images
(redis, api) resolve service names fine; only the pinned old busybox fails. Proof:

```bash
NET=$(docker inspect rpc-postgres-1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run --rm --network "$NET" busybox:1.33 nc -zv postgres 5432 -w2   # -> nc: bad address 'postgres'
docker run --rm --network "$NET" busybox:1.37 nc -zv postgres 5432 -w2   # -> postgres (...:5432) open
```

**Fix (applied):** the pin was bumped `busybox:1.33` → `1.37` in
`docker-compose.e2e.yml`. If "Build stack" ever hangs on `nc: bad address` again
with an even newer Docker, the same diagnostic applies — bump busybox further. A
hung "Build stack" is a stack/tooling issue, not your feature diff's fault.
(Longer term, a bounded retry on the wait loop would turn an infinite hang into a
fast failure.)

## Other gotchas

- **Stale Postgres volume** → `initdb: directory "/var/lib/postgresql/data" exists but
  is not empty` → `rpc-postgres-1` exits(1), 5432 never opens, `wait` hangs. Cause: a
  leftover volume from a previous local run. **Fix:** `just dc_e2e down -v --remove-orphans`
  before bringing the stack up (CI runners start clean, so this is a local-only trap).
- **Required env at module load** — the config graph (`src/config/connections.ts`)
  calls `env_or_fail` for **both** `APP_POSTGRES_URL` and `APP_REDIS_URL` at import
  time. Running a spec via `dc_e2e run api` with only the Postgres URL set fails with
  `Env key 'APP_REDIS_URL' not found` before any test runs. Set both.
- **`--no-check` everywhere** — tests run `deno test --no-check`; the repo has ~87
  pre-existing type errors, so `deno check` is *not* a gate. Validate with `deno lint`
  and `deno fmt --check` on changed files instead.
- **`just tu` / `just ti` take no pattern** — they run the whole suite. For one file,
  use `just test '<glob>'` (e.g. `just test '**/Foo.unit.spec.ts'`). Unit tests
  (`*.unit.spec.ts`) need no stack; integration (`*.integration.spec.ts`) do.
- The harness `makeDenoDbBeforeAfter` (`src/pdc/providers/test/`) creates throwaway
  `test_*` databases from migrations per run, so integration specs need a reachable
  Postgres but not a pre-seeded schema.

## Running integration tests locally

There is no bare local Postgres — bring the stack up first. Use the helper:

```bash
cd api
../.claude/skills/tests-e2e/scripts/run-integration.sh '**/UsersRepository.purge.integration.spec.ts'
```

It cleans stale volumes, starts the base services, waits (bypassing the flaky
busybox), and runs the given spec glob inside the api container with the right env.
Omit the glob to run the full integration suite. Run `just ci_test_posthook` (or
`just dc_e2e down -v`) to tear everything down afterwards.

## Fast triage when CI is stuck on "Build stack"

1. Confirm the code isn't at fault: `(api) Lint` and `(api) unit tests` are separate
   jobs — if they pass, only the stack build is stuck.
2. Reproduce locally: `just dc_e2e down -v --remove-orphans` then `just ci_test_prehook`.
   If it loops on `nc: bad address '<svc>'`, it's the busybox DNS bug above.
3. If `rpc-postgres-1` isn't in `docker ps`, read `docker logs rpc-postgres-1` — most
   often the stale-volume `initdb` error.
