"""
Exporter les tables de la zone raw vers des fichiers Parquet sur S3, pour archivage ou partage.

Usage:
    cd sqlmesh && python -m scripts.export_raw          
"""

from utils.duckdb import duckdb_client, export_tables

conn = duckdb_client()
TABLES = [
  'incentives_2019',
  'incentives_2020',
  'incentives_2021',
  'incentives_2022',
  'incentives_2023',
  'incentives_2024',
  'incentives_2025',
  'incentives_latest',
  'journeys_2019',
  'journeys_2020',
  'journeys_2021',
  'journeys_2022',
  'journeys_2023',
  'journeys_2024',
  'journeys_2025',
  'journeys_latest',
  'cee_applications',
]
export_tables(conn, TABLES, schema='raw_zone', folder='exports', view=True)

