"""Deadlock-safe index creation macro for SQLMesh.

Creates indexes only during snapshot table creation (runtime_stage == "creating"),
not during batch inserts. This prevents deadlocks when concurrent_tasks > 1,
because CREATE INDEX takes a ShareLock that conflicts with batch INSERT RowExclusiveLock.

All index definitions are passed to a single macro call, avoiding the parser bug
where multiple @macro() post-statements break query detection (length > 1 issue).

Usage (as single post-statement after query ;):

    @create_indexes(
        'idx_name ON schema.model (col)',
        'UNIQUE uq_name ON schema.model (col1, col2)',
        'idx_gist ON schema.model USING GIST (geom)',
    );
"""

from sqlmesh import macro


@macro()
def create_indexes(evaluator, *index_defs):
    if evaluator.runtime_stage != "creating":
        return None

    stmts = []
    for idx in index_defs:
        defn = idx.this if hasattr(idx, "this") else str(idx)
        if defn.upper().startswith("UNIQUE "):
            stmts.append(f"CREATE UNIQUE INDEX IF NOT EXISTS {defn[7:]}")
        else:
            stmts.append(f"CREATE INDEX IF NOT EXISTS {defn}")

    return "; ".join(stmts) if stmts else None
