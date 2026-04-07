"""
Exporter les tables de la zone raw vers des fichiers Parquet sur S3, pour archivage ou partage.

Usage:
    cd sqlmesh && python -m scripts.export_raw          
"""

from utils.duckdb import duckdb_client, export_tables

conn = duckdb_client()
TABLES = [
  'incentives',
  'journeys',
  'operator_incentives',
  'cee_applications',
]
export_tables(conn, TABLES, schema='raw_zone', folder='exports', view=True)

