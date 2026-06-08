"""Snapshot-unique index name helper.

Post-statements that create an index with a *fixed* name collide across
snapshots: PostgreSQL index names are unique per schema, so

    CREATE INDEX IF NOT EXISTS journeys_id_index ON <new_snapshot> (_id)

is silently skipped while the previous snapshot still owns that name. Every
rebuilt snapshot is then born without indexes until the janitor drops the old
one -- by which point the `creating` stage has already run, leaving the new
snapshot permanently un-indexed (full seq scans on every downstream read).

`@index_name('base')` appends the physical snapshot's unique suffix to the base
name, so each snapshot gets its own index identifier and `IF NOT EXISTS` only
skips a genuine re-run on the same snapshot.
"""

from sqlglot import exp
from sqlmesh import macro


@macro()
def index_name(evaluator, base):
    # `base` arrives as a string literal expression (e.g. 'journeys_id_index')
    base_name = base.name if isinstance(base, exp.Expression) else str(base)

    # `this_model` resolves to the physical snapshot FQN at the `creating`
    # stage, e.g. "trusted_zone"."journeys__2404643179"
    table = exp.to_table(evaluator.this_model).name  # journeys__2404643179

    # the part after `__` is unique per snapshot; fall back to the full name
    suffix = table.split("__")[-1] if "__" in table else table

    return exp.to_identifier(f"{base_name}_{suffix}")
