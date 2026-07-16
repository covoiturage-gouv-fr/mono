#!/usr/bin/env bash
# Run API integration spec(s) locally against the compose stack, sidestepping the
# two classic traps: the stale-volume initdb failure and the busybox:1.33 wait hang.
#
# Usage (from api/):
#   ../.claude/skills/tests-e2e/scripts/run-integration.sh                 # whole suite
#   ../.claude/skills/tests-e2e/scripts/run-integration.sh '**/Foo.integration.spec.ts'
#
# Tear down afterwards with: just ci_test_posthook   (or: just dc_e2e down -v)
set -euo pipefail

GLOB="${1:-**/*.integration.spec.ts}"
PG="postgres://postgres:postgres@postgres:5432/test"

# 1. Clean stale volumes so postgres initdb doesn't choke on a non-empty data dir.
just dc_e2e down -v --remove-orphans >/dev/null 2>&1 || true

# 2. Bring up the base services the integration tests need.
POSTGRES_DB=test just dc_e2e up -d postgres redis s3 mailer dex

# 3. Wait for postgres WITHOUT the pinned busybox:1.33 (its resolver is broken against
#    modern Docker DNS). A current busybox on the same network resolves fine.
NET="$(docker inspect rpc-postgres-1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
echo "waiting for postgres on network $NET ..."
for _ in $(seq 1 60); do
  if docker run --rm --network "$NET" busybox:1.37 nc -z postgres 5432 -w2 >/dev/null 2>&1; then
    echo "postgres is up"; break
  fi
  sleep 2
done

# 4. Run the spec(s) inside the api image. Both env vars are required at config load,
#    and the entrypoint must be overridden to `just` (the default one drops into a REPL).
just dc_e2e run --rm \
  -e APP_POSTGRES_URL="$PG" \
  -e APP_REDIS_URL="redis://redis:6379" \
  --entrypoint just api test "$GLOB"
