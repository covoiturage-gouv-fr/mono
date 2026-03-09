from sqlmesh import macro


def _parse_options(columns):
    """Sépare les vraies colonnes des options encodées (name=, type=, where=)."""
    col_names = []
    opts = {"name": None, "type": None, "where": None}

    for c in columns:
        s = str(c).strip("'\"")
        if s.startswith("name="):
            opts["name"] = s[5:]
        elif s.startswith("type="):
            opts["type"] = s[5:]
        elif s.startswith("where="):
            opts["where"] = s[6:]
        else:
            col_names.append(s.replace('"', '').replace("'", ''))

    return col_names, opts


def _build_index_sql(table_str, unique, col_names, index_type_str, index_name_str, where_str):
    prefix = "uq" if unique else "idx"
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
def create_index(evaluator, table, *columns):
    """
    @create_index(@this_model, col1, col2)
    @create_index(@this_model, col1, 'name=my_index')
    @create_index(@this_model, geom, 'type=GIST', 'name=my_geom_idx')
    @create_index(@this_model, email, 'where=deleted_at IS NULL')
    """
    if evaluator.runtime_stage != "creating":
        return None

    table_str = str(table)
    col_names, opts = _parse_options(columns)

    return _build_index_sql(table_str, False, col_names, opts["type"], opts["name"], opts["where"])


@macro()
def create_unique_index(evaluator, table, *columns):
    """
    @create_unique_index(@this_model, col1, col2)
    @create_unique_index(@this_model, email, 'name=uq_email')
    @create_unique_index(@this_model, email, 'where=deleted_at IS NULL')
    """
    if evaluator.runtime_stage != "creating":
        return None

    table_str = str(table)
    col_names, opts = _parse_options(columns)

    return _build_index_sql(table_str, True, col_names, opts["type"], opts["name"], opts["where"])