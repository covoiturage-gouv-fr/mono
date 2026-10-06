import re
from datetime import datetime, timezone

from pipelines.cmd.export_perimeters import COM_EVOLUTION_SQL, dump_name, sha256_line, staging_sql


def test_staging_sql_matches_prod_columns():
  sql = staging_sql('"zone_trusted"."perimeters"', [2026, 2025])
  assert sql.startswith("CREATE TABLE geo_export.perimeters AS SELECT (ROW_NUMBER() OVER (ORDER BY year, arr))::integer AS id,")
  assert sql.endswith('FROM "zone_trusted"."perimeters" WHERE year IN (2026, 2025)')
  aliases = re.findall(r" AS (\w+)", sql.split(" SELECT ", 1)[1])
  assert aliases == [
    "id", "year", "centroid", "geom", "geom_simple", "l_arr", "arr", "l_com", "com", "l_epci", "epci",
    "l_dep", "dep", "l_reg", "reg", "l_country", "country", "l_aom", "aom", "l_reseau", "reseau",
    "pop", "surface", "valid_from", "valid_until",
  ]


def test_staging_sql_rejects_non_integer_year():
  try:
    staging_sql("t", ["2026); DROP TABLE x; --"])
  except ValueError:
    return
  raise AssertionError("année non entière acceptée")


def test_dump_name_lists_sorted_years_and_timestamp():
  now = datetime(2026, 9, 29, 8, 5, 3, tzinfo=timezone.utc)
  assert dump_name([2026], now) == "perimeters_2026.20260929T080503Z.pgdump"
  assert dump_name([2026, 2025], now) == "perimeters_2025-2026.20260929T080503Z.pgdump"


def test_com_evolution_matches_prod_columns():
  assert COM_EVOLUTION_SQL.startswith("CREATE TABLE geo_export.com_evolution AS SELECT")
  assert re.findall(r" AS (\w+)", COM_EVOLUTION_SQL.split(" SELECT ", 1)[1]) == [
    "year", "mod", "old_com", "new_com", "l_mod",
  ]


def test_sha256_line_is_sha256sum_format():
  sha = "a" * 64
  assert sha256_line(sha, "perimeters_2025-2026.20261001T080536Z.pgdump") == (
    f"{sha}  perimeters_2025-2026.20261001T080536Z.pgdump\n"
  )
