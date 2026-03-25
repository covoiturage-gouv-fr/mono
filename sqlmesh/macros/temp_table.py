from sqlmesh import macro
from sqlglot import exp

@macro()
def temp_table_name(evaluator,schema, table_name, start_ts, end_ts):
  """Génère un nom de table cohérent entre pre_statements et le SELECT"""
  def clean(ts):
    return str(ts)[:10].replace("-", "")
  return f"{schema}.{table_name}_{clean(start_ts)}_{clean(end_ts)}"

@macro()
def drop_temp_table(evaluator, table_name):
  return exp.maybe_parse(f"DROP TABLE IF EXISTS {table_name}", dialect="postgres")

@macro()
def create_temp_table(evaluator, table_name, query):
  
  statements = [
    f"DROP TABLE IF EXISTS {table_name}",
    f"CREATE UNLOGGED TABLE {table_name} AS ({query})",
  ]
  return [exp.maybe_parse(s, dialect="postgres") for s in statements]
