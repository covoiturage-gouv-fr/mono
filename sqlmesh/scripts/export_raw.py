from duckdb import duckdb_client, export_tables

conn = duckdb_client()
TABLES = [
  'aires_covoiturage'
]
export_tables(conn, TABLES, folder='test')

