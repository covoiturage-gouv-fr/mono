"""Datalake DB migration runner.

Applies ordered, idempotent SQL files that live in `datalake/migrations/` to the
datalake Postgres. This owns the DB objects dbt does NOT manage — custom functions
(e.g. `ts_ceil`), grants, etc. — the prerequisite layer every model build assumes.

Runs at each deployment (a k8s Job in the ops repo invokes `just migrate` before
the app/pipeline pods start). Idempotent: applied versions are tracked in a ledger
table, so re-running is a no-op.

Privileged / cross-DB plumbing (role creation, the FDW server + IMPORT FOREIGN
SCHEMA) stays in the ops repo (OpenTofu) — this runner assumes those exist.
"""

import os
from pathlib import Path

import re

import psycopg
import typer
from dotenv import load_dotenv
from psycopg import pq

from pipelines.helpers.pg import pg_conninfo

load_dotenv()
app = typer.Typer()

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
LEDGER = "public.schema_migrations"
# version = migration filename stem, inlined into the ledger INSERT (simple protocol).
# It is always a repo-shipped filename, never user input — this asserts that invariant.
_VERSION_RE = re.compile(r"^[A-Za-z0-9_]+$")


def discover(migrations_dir: Path) -> list[Path]:
    """Return migration files sorted by name (numeric prefix drives order)."""
    return sorted(Path(migrations_dir).glob("*.sql"))


def select_pending(files: list[Path], applied: set[str]) -> list[str] | list[Path]:
    """Files whose version (filename stem) has not been applied yet."""
    return [f for f in files if f.stem not in applied]


def _ensure_ledger(conn, ledger: str) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {ledger} "
        "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )


def _applied_versions(conn, ledger: str) -> set[str]:
    return {r[0] for r in conn.execute(f"SELECT version FROM {ledger}").fetchall()}


def _apply(conn, version: str, sql: str, ledger: str) -> None:
    """Apply one migration atomically via the simple-query protocol.

    The simple protocol supports multiple statements and $$-quoted function bodies
    in a single call — the extended protocol (conn.execute) does not. `version` is
    an internal filename stem (never user input), safe to inline.
    """
    if not _VERSION_RE.match(version):
        raise ValueError(f"invalid migration version: {version!r}")
    script = f"BEGIN;\n{sql}\nINSERT INTO {ledger} (version) VALUES ('{version}');\nCOMMIT;"
    res = conn.pgconn.exec_(script.encode())
    if res.status not in (pq.ExecStatus.COMMAND_OK, pq.ExecStatus.TUPLES_OK):
        conn.pgconn.exec_(b"ROLLBACK;")
        raise RuntimeError(f"migration {version} failed: {res.error_message.decode().strip()}")


def run_migrations(conn, migrations_dir: Path, ledger: str = LEDGER) -> list[str]:
    _ensure_ledger(conn, ledger)
    applied = _applied_versions(conn, ledger)
    done: list[str] = []
    for f in select_pending(discover(migrations_dir), applied):
        _apply(conn, f.stem, f.read_text(), ledger)
        done.append(f.stem)
        print(f"✅ applied {f.stem}")
    if not done:
        print("✅ no pending migrations")
    return done


@app.command()
def migrate(migrations_dir: str = str(MIGRATIONS_DIR)):
    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        run_migrations(conn, Path(migrations_dir))


if __name__ == "__main__":
    app()
