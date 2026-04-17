import duckdb
from typing import Optional


def create_schema(conn: duckdb.DuckDBPyConnection, schema: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{schema};")


def get_existing_tables(conn: duckdb.DuckDBPyConnection, schema: str) -> set[str]:
    rows = conn.execute(f"""
        SELECT table_name
        FROM pg.information_schema.tables
        WHERE table_schema = '{schema}';
    """).fetchall()
    return {r[0] for r in rows}

def drop_table(conn: duckdb.DuckDBPyConnection, schema: str, table: str):
    conn.execute(f"DROP TABLE IF EXISTS pg.{schema}.{table};")


def build_select(select: Optional[list[str | list[str]]]) -> str:
    if not select:
        return "*"
    sql = []
    for item in select:
        if isinstance(item, list):
            col, dtype = item
            sql.append(f"CAST({col} AS {dtype.upper()}) AS {col}")
        else:
            sql.append(item)
    return ", ".join(sql)