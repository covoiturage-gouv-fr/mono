# macros/get_latest_millesime_or.py
from sqlmesh import macro
from sqlglot import exp

@macro()
def get_millesime_or_latest(evaluator, year):
    return exp.maybe_parse(f"""
      (
        SELECT COALESCE(
          (SELECT MAX(year) FROM trusted_zone.perimeters WHERE year = {year}),
          (SELECT MAX(year) FROM trusted_zone.perimeters)
        )
      )
    """)