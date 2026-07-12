"""Fragments SQL partagés des endpoints observatoire.

Deux briques réutilisées par toutes les familles (flux, occupation, distribution,
incentive, keyfigures) :

- `resolve_grain` : choix du suffixe de table selon le param temporel présent
  (priorité month > trimester > semester > year), avec le renommage legacy
  `trimester` -> table `quarter` côté datalake.
- `perimeter_in_subquery` : porte la sous-requête `code IN (SELECT <observe> FROM
  geo.perimeters WHERE <type>=<code>)` de l'API Deno vers `zone_exposed.observatory_perimeters`
  (l'API ne lit que la zone exposée). Le millésime reproduit `get_latest_millesime_or`.

`type`/`observe` sont des **noms de colonnes** interpolés : ils sont validés par
`check_territory_param` (allowlist) avant d'arriver ici. Les valeurs (year, code, …)
sont passées en paramètres psycopg `%(...)s`.
"""

PERIMETERS_TABLE = "zone_exposed.observatory_perimeters"


def resolve_grain(month: int | None, trimester: int | None,
                  semester: int | None) -> tuple[str, str | None, int | None]:
    """(suffixe_table, colonne_temporelle, valeur) selon le grain demandé.

    Priorité stricte month > trimester > semester > year (comme getTableName).
    Le suffixe suit les tables datalake : month/quarter/semester/year.
    """
    if month is not None:
        return "month", "month", month
    if trimester is not None:
        return "quarter", "quarter", trimester
    if semester is not None:
        return "semester", "semester", semester
    return "year", None, None


def perimeter_in_subquery(observe_col: str, type_col: str) -> str:
    """Sous-requête `IN (...)` des codes `observe` contenus dans (type=code).

    Reproduit `SELECT arr AS com, epci, ... FROM geo.perimeters` de l'API Deno,
    en lisant `zone_exposed.observatory_perimeters`. Le millésime = celui de l'année
    demandée s'il existe, sinon le plus récent (get_latest_millesime_or).

    `observe_col`/`type_col` doivent être pré-validés (allowlist territoire).
    """
    return f"""(
      SELECT t.{observe_col}
      FROM (
        SELECT arr AS com, epci, aom, dep, reg, country
        FROM {PERIMETERS_TABLE}
        WHERE year = (
          SELECT year FROM (
            SELECT max(year) AS year FROM {PERIMETERS_TABLE} WHERE year = %(year)s
            UNION ALL
            SELECT max(year) AS year FROM {PERIMETERS_TABLE}
            ORDER BY year
            LIMIT 1
          ) m
        )
      ) t
      WHERE t.{type_col} = %(code)s
    )"""
