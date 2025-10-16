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


def auto_cast(df: pd.DataFrame, columns: dict[str, str], date_format: t.Optional[str] = None) -> pd.DataFrame:
    """
    Cast les colonnes selon le type SQL désiré.
    Si df est un GeoDataFrame et contient une colonne géométrique,
    elle sera automatiquement convertie en WKB (BYTEA).
    """
    def to_bool(s: pd.Series) -> pd.Series:
      return (
          s.apply(
              lambda x: (
                  True if str(x).strip().lower() in ["true", "1", "yes", "y", "vrai"]
                  else False if str(x).strip().lower() in ["false", "0", "no", "n", "faux"]
                  else pd.NA
              )
          ).astype("boolean").astype(object)
      )
    def na_to_null(s: pd.Series, string: bool = False) -> pd.Series:
      if string:
        return s.where(pd.notna(s), None).map(lambda x: str(x) if x is not None else None)
      else:
        return s.where(pd.notna(s), None).map(lambda x: x if x is not None else None)

    CAST_MAPPING = {
      "DATE": lambda s: na_to_null(pd.to_datetime(s, format=date_format, errors="coerce").dt.date),
      "TIMESTAMP": lambda s: na_to_null(pd.to_datetime(s, errors="coerce")),
      "INTEGER": lambda s: na_to_null(pd.to_numeric(s, errors="coerce")),
      "BIGINT": lambda s: na_to_null(pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce")),
      "FLOAT": lambda s: na_to_null(pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce")),
      "DOUBLE": lambda s: na_to_null(pd.to_numeric(s.astype(str).str.replace(',', '.'), errors="coerce")),
      "VARCHAR": lambda s: na_to_null(s, string=True),
      "TEXT": lambda s: na_to_null(s, string=True),
      "BOOLEAN": to_bool,
    }

    for col, sql_type in columns.items():
        if col not in df.columns:
            df[col] = pd.NA
        sql_type_clean = sql_type.upper().split("(")[0]
        caster = CAST_MAPPING.get(sql_type_clean)
        if caster:
            try:
                df[col] = caster(df[col])
            except Exception as e:
                # 🧭 Log détaillé pour diagnostic
                print(f"[ERROR] ❌ Erreur de cast sur '{col}' → {sql_type_clean}")
                print(f"        Dtype actuel: {df[col].dtype}")
                print(f"        Erreur: {e}")
    df = df.where(pd.notnull(df), None)
    return df