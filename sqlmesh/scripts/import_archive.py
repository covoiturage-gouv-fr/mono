"""
Importer les fichiers Parquet depuis S3 vers la zone archive.

Usage:
    cd sqlmesh && python -m scripts.import_archive          
"""
from utils.duckdb import duckdb_client, import_tables

conn = duckdb_client()
TABLES = [
  'incentives',
  'journeys',
  'operator_incentives',
  'cee_applications',
]

import_tables(conn, TABLES, schema='archive_zone', folder='exports')

