from pipelines.cmd.fdw_sync import import_statement


def test_import_statement_skips_existing_tables():
  stmt = import_statement("covoiturage_fdw", "dlk_export", "dlk_import", ["carpool_v2_geo", "territory_territory_group"])
  assert stmt.as_string(None) == (
    'IMPORT FOREIGN SCHEMA "dlk_export" EXCEPT ("carpool_v2_geo", "territory_territory_group") '
    'FROM SERVER "covoiturage_fdw" INTO "dlk_import"'
  )


def test_import_statement_imports_everything_on_empty_schema():
  stmt = import_statement("covoiturage_fdw", "dlk_export", "dlk_import", [])
  assert stmt.as_string(None) == 'IMPORT FOREIGN SCHEMA "dlk_export" FROM SERVER "covoiturage_fdw" INTO "dlk_import"'
