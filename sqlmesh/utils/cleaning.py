# utils/cleaning.py
import pandas as pd
import unicodedata, re

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
  def normalize(col: str) -> str:
    col = col.strip()
    col = "".join(c for c in unicodedata.normalize("NFKD", col) if not unicodedata.combining(c))
    col = col.lower().replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col
  return df.rename(columns={c: normalize(c) for c in df.columns})

# mapping SQL type -> fonction de cast pandas


def auto_cast(df: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
  CAST_MAPPING = {
    "DATE": lambda s: pd.to_datetime(s, format="%d/%m/%Y", errors="coerce").dt.date,
    "TIMESTAMP": lambda s: pd.to_datetime(s, errors="coerce"),
    "INTEGER": lambda s: pd.to_numeric(s, errors="coerce"),
    "BIGINT": lambda s: pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce"),
    "FLOAT": lambda s: pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce"),
    "DOUBLE": lambda s: pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce"),
    "VARCHAR": lambda s: s.where(pd.notna(s), None).map(lambda x: str(x) if x is not None else None),
    "TEXT": lambda s: s.where(pd.notna(s), None).map(lambda x: str(x) if x is not None else None),
    "BOOLEAN": lambda s: s.astype(str).str.strip().str.lower().map(
        {"true": True, "1": True, "yes": True, "false": False, "0": False, "no": False, "nan": pd.NA}
    ).astype("boolean"),
  }
  for col, sql_type in columns.items():
      if col not in df.columns:
          df[col] = pd.NA
      sql_type_clean = sql_type.upper().split("(")[0]  # VARCHAR(255) -> VARCHAR
      caster = CAST_MAPPING.get(sql_type_clean)
      if caster:
          try:
              df[col] = caster(df[col])
          except Exception as e:
              print(f"[WARN] Impossible de caster {col} en {sql_type_clean}: {e}")
  return df