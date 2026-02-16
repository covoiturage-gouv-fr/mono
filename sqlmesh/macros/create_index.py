# macros/create_index.py
from sqlmesh import macro


def _build_index_sql(table_str, unique, col_names, index_type_str, index_name_str, where_str):
    prefix   = "uq" if unique else "idx"
    idx_name = index_name_str or f"{prefix}_{'_'.join(col_names)}"
    if len(idx_name) > 63:
        idx_name = idx_name[:63]

    unique_clause = "UNIQUE " if unique else ""
    using_clause  = f"USING {index_type_str.upper()} " if index_type_str else ""
    cols_clause   = ", ".join(col_names)
    where_clause  = f" WHERE {where_str}" if where_str else ""

    return (
        f"CREATE {unique_clause}INDEX IF NOT EXISTS {idx_name} "
        f"ON {table_str} {using_clause}({cols_clause}){where_clause}"
    )


@macro()
def create_index(evaluator, table, *columns, index_type=None, index_name=None, where=None):
    """
    @CREATE_INDEX(@this_model, col1, col2)
    @CREATE_INDEX(@this_model, geom, index_type='GIST')
    """
    if evaluator.runtime_stage != "creating":
        return None

    table_str      = str(table)
    col_names      = [str(c).replace('"', '').replace("'", '') for c in columns]
    index_type_str = str(index_type).strip("'\"") if index_type else None
    index_name_str = str(index_name).strip("'\"") if index_name else None
    where_str      = str(where).strip("'\"") if where else None

    return _build_index_sql(table_str, False, col_names, index_type_str, index_name_str, where_str)


@macro()
def create_unique_index(evaluator, table, *columns, index_type=None, index_name=None, where=None):
    """
    @CREATE_UNIQUE_INDEX(@this_model, col1, col2, col3)
    @CREATE_UNIQUE_INDEX(@this_model, email, where='deleted_at IS NULL')
    """
    if evaluator.runtime_stage != "creating":
        return None

    table_str      = str(table)
    col_names      = [str(c).replace('"', '').replace("'", '') for c in columns]
    index_type_str = str(index_type).strip("'\"") if index_type else None
    index_name_str = str(index_name).strip("'\"") if index_name else None
    where_str      = str(where).strip("'\"") if where else None

    return _build_index_sql(table_str, True, col_names, index_type_str, index_name_str, where_str)