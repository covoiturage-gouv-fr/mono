import json
import typing as t
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

def _clean_scalar(s: pd.Series) -> pd.Series:
    return s.where(pd.notna(s), None).astype(object)

def _cast_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").where(pd.notna(s), None)

def _cast_bool(s: pd.Series) -> pd.Series:
  TRUE_SET = {"true", "1", "yes", "y", "vrai"}
  FALSE_SET = {"false", "0", "no", "n", "faux"}
  def cast_value(v):
      if v is None:
          return None
      if isinstance(v, float) and pd.isna(v):
          return None
      if isinstance(v, str):
          v_clean = v.strip().lower()
          if v_clean in TRUE_SET:
              return True
          elif v_clean in FALSE_SET:
              return False
          else:
              return None
      if isinstance(v, (int, bool)):
          return bool(v)
      return None
  return s.apply(cast_value).astype(object)


def _cast_date(s: pd.Series, date_format: t.Optional[str] = None, default: t.Optional[str] = None) -> pd.Series:
    result = pd.to_datetime(s, format=date_format, errors='coerce')
    return pd.Series(
        [x.date() if pd.notna(x) else default for x in result],
        index=s.index,
        dtype=object
    )

def _cast_timestamp( s: pd.Series, date_format: t.Optional[str] = None,  default: t.Optional[str] = None) -> pd.Series:
    result = pd.to_datetime(s, format=date_format, errors='coerce')
    return pd.Series(
        [x.to_pydatetime() if pd.notna(x) else default for x in result],
        index=s.index,
        dtype=object
    )

def _cast_json(s: pd.Series) -> pd.Series:
    values = []
    for v in s:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            values.append(None)
        elif isinstance(v, (dict, list)):
            values.append(json.dumps(v, ensure_ascii=False))
        else:
            values.append(str(v))
    return pd.Series(values, index=s.index, dtype=object)


def _cast_array(s: pd.Series, inner_type: str = "str") -> pd.Series:
    values = []
    caster = {
        "str": str,
        "int": lambda x: int(x) if x is not None else None,
        "float": lambda x: float(x) if x is not None else None,
        "bool": lambda x: bool(x) if x is not None else None
    }[inner_type]
    for v in s:
        if v is None:
            values.append(None)
        elif isinstance(v, (list, tuple)):
            casted = []
            for e in v:
                try:
                    casted.append(caster(e))
                except Exception:
                    casted.append(None)
            values.append(casted)
        else:
            try:
                values.append([caster(v)])
            except Exception:
                values.append([None])
    return pd.Series(values, index=s.index, dtype=object)

def auto_cast(df: pd.DataFrame, sql_types: dict[str, str], date_format: t.Optional[str] = None, date_default: t.Optional[str] = None) -> pd.DataFrame:
    df = df.copy()
    NUMERIC_TYPES = {"SMALLINT", "BIGINT", "INTEGER", "FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE PRECISION", "DOUBLE"}
    DATE_TYPES = {"DATE"}
    TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMPTZ"}
    BOOL_TYPES = {"BOOLEAN"}
    JSON_TYPES = {"JSON", "JSONB"}
    for col, sql_type in sql_types.items():
        if col not in df.columns:
            df[col] = None
            continue
        t = sql_type.upper()
        # ARRAY
        if t.endswith("[]"):
            inner_type = "str"  # fallback
            if "CHAR" in t or "TEXT" in t or "VARCHAR" in t:
                inner_type = "str"
            elif "INT" in t or "BIGINT" in t:
                inner_type = "int"
            elif "FLOAT" in t or "DOUBLE" in t or "REAL" in t:
                inner_type = "float"
            elif "BOOL" in t:
                inner_type = "bool"
            df[col] = _cast_array(df[col], inner_type=inner_type)
            continue
        # JSON
        if t in JSON_TYPES:
            df[col] = _cast_json(df[col])
            continue
        # Scalars
        if t in NUMERIC_TYPES:
            df[col] = _cast_numeric(df[col])
        elif t in BOOL_TYPES:
            df[col] = _cast_bool(df[col])
        elif t in DATE_TYPES:
            df[col] = _cast_date(df[col], date_format=date_format, default=date_default)
        elif t in TIMESTAMP_TYPES:
            df[col] = _cast_timestamp(df[col], date_format=date_format, default=date_default)
        else:
            # VARCHAR, TEXT, UUID, fallback
            df[col] = _clean_scalar(df[col])
    return df
