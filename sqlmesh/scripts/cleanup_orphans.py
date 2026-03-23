"""
Identify orphaned SQLMesh snapshot tables and views in PostgreSQL.

Compares current physical table names from sqlmesh.Context against
actual tables/views in raw_zone, trusted_zone, refined_zone schemas.

Usage:
    cd sqlmesh && python scripts/cleanup_orphans.py          # dry-run
    cd sqlmesh && python scripts/cleanup_orphans.py --apply   # execute drops
"""

import argparse
import sqlmesh

SCHEMAS = ["raw_zone", "trusted_zone", "refined_zone"]


def get_current_physical_names(ctx: sqlmesh.Context) -> set[str]:
    """Get all current physical table names from SQLMesh state."""
    names = set()
    for fqn in ctx.models:
        physical = ctx.table_name(fqn)
        # format: catalog.schema.table -> keep schema.table
        parts = physical.split(".")
        if len(parts) == 3:
            names.add(f"{parts[1]}.{parts[2]}")
        else:
            names.add(physical)
    return names


def get_model_base_names(ctx: sqlmesh.Context) -> set[str]:
    """Get clean view names (schema.model) for the virtual layer."""
    names = set()
    for fqn in ctx.models:
        # FQN: "catalog"."schema"."model"
        clean = fqn.replace('"', "")
        parts = clean.split(".")
        if len(parts) == 3:
            names.add(f"{parts[1]}.{parts[2]}")
    return names


def get_db_objects(adapter) -> tuple[set[str], set[str]]:
    """Query pg_tables and pg_views for our schemas."""
    tables = set()
    views = set()

    schema_list = "', '".join(SCHEMAS)

    df = adapter.fetchdf(
        f"SELECT schemaname, tablename FROM pg_tables "
        f"WHERE schemaname IN ('{schema_list}')"
    )
    for _, row in df.iterrows():
        tables.add(f"{row['schemaname']}.{row['tablename']}")

    df = adapter.fetchdf(
        f"SELECT schemaname, viewname FROM pg_views "
        f"WHERE schemaname IN ('{schema_list}')"
    )
    for _, row in df.iterrows():
        views.add(f"{row['schemaname']}.{row['viewname']}")

    return tables, views


def quote_identifier(schema_table: str) -> str:
    """Quote schema.table for safe SQL output."""
    schema, table = schema_table.split(".", 1)
    return f'{schema}."{table}"'


def main():
    parser = argparse.ArgumentParser(description="Find and drop orphaned SQLMesh snapshots")
    parser.add_argument("--apply", action="store_true", help="Execute DROP statements")
    args = parser.parse_args()

    ctx = sqlmesh.Context(paths=["."])
    adapter = ctx.engine_adapter

    current_physical = get_current_physical_names(ctx)
    model_base_names = get_model_base_names(ctx)
    db_tables, db_views = get_db_objects(adapter)

    # Find orphaned tables: in DB but not in current physical set
    orphaned_tables = sorted(
        t for t in db_tables if t not in current_physical
    )

    # Find orphaned views: not a current snapshot and not a virtual layer view
    orphaned_views = sorted(
        v for v in db_views
        if v not in current_physical and v not in model_base_names
    )

    print(f"Current physical tables: {len(current_physical)}")
    print(f"Current model views: {len(model_base_names)}")
    print(f"DB tables in schemas: {len(db_tables)}")
    print(f"DB views in schemas: {len(db_views)}")
    print()

    if orphaned_tables:
        print(f"-- Orphaned tables ({len(orphaned_tables)})")
        for t in orphaned_tables:
            stmt = f"DROP TABLE IF EXISTS {quote_identifier(t)} CASCADE"
            print(f"{stmt};")
            if args.apply:
                adapter.execute(stmt)
    else:
        print("-- No orphaned tables found")

    print()

    if orphaned_views:
        print(f"-- Orphaned views ({len(orphaned_views)})")
        for v in orphaned_views:
            stmt = f"DROP VIEW IF EXISTS {quote_identifier(v)} CASCADE"
            print(f"{stmt};")
            if args.apply:
                adapter.execute(stmt)
    else:
        print("-- No orphaned views found")

    if args.apply and (orphaned_tables or orphaned_views):
        total = len(orphaned_tables) + len(orphaned_views)
        print(f"\nDropped {total} orphaned objects.")


if __name__ == "__main__":
    main()
