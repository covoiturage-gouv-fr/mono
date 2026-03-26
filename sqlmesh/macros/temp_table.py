from sqlmesh import macro
from sqlglot import exp

@macro()
def drop_temp_table(evaluator, table_name):
  return exp.maybe_parse(f"DROP TABLE IF EXISTS {table_name}", dialect="postgres")

@macro()
def create_temp_table(evaluator, table_name, query):
  
  statements = [
    f"DROP TABLE IF EXISTS {table_name}",
    f"CREATE TEMP TABLE {table_name} AS ({query})",
  ]
  return [exp.maybe_parse(s, dialect="postgres") for s in statements]
