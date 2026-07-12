"""Requêtes des endpoints observatoire agrégés (flux, occupation, distribution,
incentive, keyfigures, infra) sur la zone exposée.

Porté depuis `api/src/pdc/services/observatory/providers/*`. Chaque `build_*`
renvoie `(sql, params)` — pur et testable sans base. Les valeurs sont liées
(`%(...)s`) ; `type`/`observe`/`indic` sont des noms de colonnes interpolés,
validés par allowlist avant d'arriver ici (défense contre l'injection).

Différences structurelles avec le legacy, assumées et documentées :
- Les modèles exposés `occupation_*`, `incentive_*`, `users_*` proviennent de
  `territory_month_<type>_both` : ils n'ont **pas** de dimension `direction`
  (direction = `both` seulement). Le filtre `direction` legacy est donc ignoré
  pour ces familles ; `direction='both'` est émis en dur pour préserver la forme.
- Le grain `trimester` (param API) mappe la table `_quarter` (colonne `quarter`).
"""

from ..helpers import check_territory_param
from ..observatory_sql import perimeter_in_subquery, resolve_grain

# indics interpolés (noms de colonnes) -> allowlist par famille, fallback legacy.
_FLUX_INDICS = ("journeys", "passengers", "distance", "duration")
_OCCUPATION_INDICS = ("journeys", "trips", "has_incentive", "occupation_rate")


def _check_indic(indic: str | None, allowed: tuple[str, ...]) -> str:
    return indic if indic in allowed else "journeys"


def normalize_flux_indic(indic: str | None) -> str:
    """indic de flux normalisé (allowlist) — pour la clé de cache et le SQL."""
    return _check_indic(indic, _FLUX_INDICS)


def normalize_occupation_indic(indic: str | None) -> str:
    return _check_indic(indic, _OCCUPATION_INDICS)


async def fetch(conn, sql: str, params: dict) -> list[dict]:
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        return list(await cur.fetchall())


# --------------------------------------------------------------------------- #
# FLUX
# --------------------------------------------------------------------------- #

def build_flux(type_: str, observe: str, code: str, year: int,
               month: int | None = None, trimester: int | None = None,
               semester: int | None = None) -> tuple[str, dict]:
    """Flux OD entre territoires (porté de getFlux)."""
    observe = check_territory_param(observe)
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    perim = perimeter_in_subquery(observe, type_)

    where = [
        "type = %(observe)s",
        "(distance / journeys) <= 80",
        f"(territory_1 IN {perim} OR territory_2 IN {perim})",
        "territory_1 <> territory_2",
        "year = %(year)s",
    ]
    params = {"year": year, "code": code, "observe": observe}
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT
        l_territory_1 AS ter_1, lng_1, lat_1,
        l_territory_2 AS ter_2, lng_2, lat_2,
        passengers, distance, duration
      FROM zone_exposed.od_{suffix}
      WHERE {' AND '.join(where)}
    """
    return sql, params


def build_best_flux(type_: str, code: str, year: int, limit: int = 10,
                    month: int | None = None, trimester: int | None = None,
                    semester: int | None = None) -> tuple[str, dict]:
    """Meilleurs flux d'un territoire (porté de getBestFlux). Périmètre = grain com."""
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    perim = perimeter_in_subquery("com", type_)

    where = [
        "year = %(year)s",
        f"(territory_1 IN {perim} OR territory_2 IN {perim})",
    ]
    params = {"year": year, "code": code, "limit": limit}
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT DISTINCT territory_1, l_territory_1, territory_2, l_territory_2, journeys
      FROM zone_exposed.od_{suffix}
      WHERE {' AND '.join(where)}
      ORDER BY journeys DESC
      LIMIT %(limit)s
    """
    return sql, params


def build_evol_flux(type_: str, code: str, indic: str, past: int = 2,
                    month: int | None = None, trimester: int | None = None,
                    semester: int | None = None) -> tuple[str, dict]:
    """Évolution temporelle d'un indicateur de flux (porté de getEvolFlux).

    Pas de jointure périmètre : filtre direct territory_1/territory_2 = code.
    `has_incentive` (indic legacy) n'existe pas dans od_* -> retombe sur journeys.
    """
    type_ = check_territory_param(type_)
    indic = _check_indic(indic, _FLUX_INDICS)
    suffix, tcol, _ = resolve_grain(month, trimester, semester)
    limit = past * 12 + 1

    cols = ["year", f"sum({indic}::numeric) AS {indic}"]
    group = ["year"]
    if tcol:
        cols.append(tcol)
        group.append(tcol)
    if indic == "distance":
        cols.append("sum(journeys) AS journeys")
    sql = f"""
      SELECT {', '.join(cols)}
      FROM zone_exposed.od_{suffix}
      WHERE type = %(type)s AND (territory_1 = %(code)s OR territory_2 = %(code)s)
      GROUP BY {', '.join(group)}
      ORDER BY ({', '.join(group)}) DESC
      LIMIT %(limit)s
    """
    return sql, {"type": type_, "code": code, "limit": limit}


# --------------------------------------------------------------------------- #
# OCCUPATION
# --------------------------------------------------------------------------- #

def build_occupation(type_: str, observe: str, code: str, year: int,
                     month: int | None = None, trimester: int | None = None,
                     semester: int | None = None) -> tuple[str, dict]:
    """Taux d'occupation par territoire (porté de getOccupation).

    Le modèle exposé est `both`-only : le filtre `direction` legacy est ignoré.
    """
    observe = check_territory_param(observe)
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    perim = perimeter_in_subquery(observe, type_)

    where = [
        "year = %(year)s",
        "type = %(observe)s",
        f"code IN {perim}",
    ]
    params = {"year": year, "code": code, "observe": observe}
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT year, type, code, libelle, journeys, occupation_rate, geom
      FROM zone_exposed.occupation_{suffix}
      WHERE {' AND '.join(where)}
    """
    return sql, params


def build_best_territories(type_: str, observe: str, code: str, year: int,
                           limit: int = 10, month: int | None = None,
                           trimester: int | None = None,
                           semester: int | None = None) -> tuple[str, dict]:
    """Meilleurs territoires par trajets (porté de getBestTerritories, direction=both)."""
    observe = check_territory_param(observe)
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    perim = perimeter_in_subquery(observe, type_)

    where = [
        "year = %(year)s",
        "type = %(observe)s",
        f"code IN {perim}",
    ]
    params = {"year": year, "code": code, "limit": limit, "observe": observe}
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT code, libelle, journeys
      FROM zone_exposed.occupation_{suffix}
      WHERE {' AND '.join(where)}
      ORDER BY journeys DESC
      LIMIT %(limit)s
    """
    return sql, params


def build_evol_occupation(type_: str, code: str, indic: str, past: int = 2,
                          month: int | None = None, trimester: int | None = None,
                          semester: int | None = None) -> tuple[str, dict]:
    """Évolution temporelle d'un indicateur d'occupation (porté, direction=both)."""
    type_ = check_territory_param(type_)
    indic = _check_indic(indic, _OCCUPATION_INDICS)
    suffix, tcol, _ = resolve_grain(month, trimester, semester)
    limit = past * 12 + 1

    cols = ["year", f"{indic}::float AS {indic}"]
    order = ["year"]
    if tcol:
        cols.append(tcol)
        order.append(tcol)
    sql = f"""
      SELECT {', '.join(cols)}
      FROM zone_exposed.occupation_{suffix}
      WHERE type = %(type)s AND code = %(code)s
      ORDER BY ({', '.join(order)}) DESC
      LIMIT %(limit)s
    """
    return sql, {"type": type_, "code": code, "limit": limit}


# --------------------------------------------------------------------------- #
# DISTRIBUTION
# --------------------------------------------------------------------------- #

def _distribution(column: str, type_: str, code: str, year: int, direction: str | None,
                  month, trimester, semester) -> tuple[str, dict]:
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    where = ["year = %(year)s", "type = %(type)s", "code = %(code)s"]
    params = {"year": year, "type": type_, "code": code}
    if direction is not None:
        where.append("direction = %(direction)s")
        params["direction"] = direction
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT code, libelle, direction, {column}
      FROM zone_exposed.distribution_{suffix}
      WHERE {' AND '.join(where)}
    """
    return sql, params


def build_journeys_by_hours(type_: str, code: str, year: int,
                            month=None, trimester=None, semester=None) -> tuple[str, dict]:
    """Distribution horaire (porté de getJourneysByHours). Toutes directions."""
    return _distribution("hours", type_, code, year, None, month, trimester, semester)


def build_journeys_by_distances(type_: str, code: str, year: int, direction: str,
                                month=None, trimester=None, semester=None) -> tuple[str, dict]:
    """Distribution kilométrique (porté de getJourneysByDistances). Direction requise."""
    return _distribution("distances", type_, code, year, direction, month, trimester, semester)


# --------------------------------------------------------------------------- #
# INCENTIVE
# --------------------------------------------------------------------------- #

def build_incentive(type_: str, code: str, year: int,
                    month=None, trimester=None, semester=None) -> tuple[str, dict]:
    """Répartition des incitations (porté de getIncentive, direction=both)."""
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)
    where = ["year = %(year)s", "type = %(type)s", "code = %(code)s"]
    params = {"year": year, "type": type_, "code": code}
    if tcol:
        where.append(f"{tcol} = %(tval)s")
        params["tval"] = tval
    sql = f"""
      SELECT code, libelle, 'both'::text AS direction, collectivite, operateur, autres
      FROM zone_exposed.incentive_{suffix}
      WHERE {' AND '.join(where)}
    """
    return sql, params


# --------------------------------------------------------------------------- #
# KEYFIGURES (recomposition, direction=both)
# --------------------------------------------------------------------------- #

def build_keyfigures(type_: str, code: str, year: int,
                     month=None, trimester=None, semester=None) -> tuple[str, dict]:
    """Chiffres clés d'un territoire (recomposition — pas de modèle exposé dédié).

    Compose od_* (passengers/distance/duration sommés + intra), occupation_*
    (journeys, occupation_rate, has_incentive) et users_* (new_drivers/passengers).
    Direction `both` uniquement (modèles exposés sans dimension direction).
    """
    type_ = check_territory_param(type_)
    suffix, tcol, tval = resolve_grain(month, trimester, semester)

    od_time = f"AND {tcol} = %(tval)s" if tcol else ""
    occ_time = f"AND o.{tcol} = %(tval)s" if tcol else ""
    users_join_time = f"AND u.{tcol} = o.{tcol}" if tcol else ""
    params = {"year": year, "type": type_, "code": code}
    if tcol:
        params["tval"] = tval

    sql = f"""
      WITH od_agg AS (
        SELECT
          sum(passengers)::int AS passengers,
          sum(distance)::int   AS distance,
          sum(duration)::int   AS duration
        FROM zone_exposed.od_{suffix}
        WHERE type = %(type)s AND year = %(year)s {od_time}
          AND (territory_1 = %(code)s OR territory_2 = %(code)s)
      ),
      intra AS (
        SELECT sum(journeys)::int AS intra_journeys
        FROM zone_exposed.od_{suffix}
        WHERE type = %(type)s AND year = %(year)s {od_time}
          AND territory_1 = %(code)s AND territory_2 = %(code)s
      )
      SELECT
        o.code, o.libelle, 'both'::text AS direction,
        od_agg.passengers, od_agg.distance, od_agg.duration,
        o.journeys::int AS journeys,
        intra.intra_journeys,
        o.has_incentive::int AS has_incentive,
        o.occupation_rate::float AS occupation_rate,
        u.new_drivers::int AS new_drivers,
        u.new_passengers::int AS new_passengers
      FROM zone_exposed.occupation_{suffix} o
      LEFT JOIN zone_exposed.users_{suffix} u
        ON u.type = o.type AND u.year = o.year AND u.code = o.code {users_join_time}
      CROSS JOIN od_agg
      CROSS JOIN intra
      WHERE o.type = %(type)s AND o.code = %(code)s AND o.year = %(year)s {occ_time}
    """
    return sql, params


# --------------------------------------------------------------------------- #
# INFRA (aires de covoiturage)
# --------------------------------------------------------------------------- #

def build_aires_covoiturage(type_: str, code: str | None = None) -> tuple[str, dict]:
    """Aires de covoiturage ouvertes (porté de getAiresCovoiturage).

    Lit `zone_exposed.aires_covoiturage` (déjà filtré ouvert=true, geom en GeoJSON).
    Filtre territorial optionnel via `observatory_perimeters` (comme geo.perimeters).
    """
    type_ = check_territory_param(type_)
    where = ["true"]
    params: dict = {}
    if code:
        where.append(f"""insee IN (
          SELECT arr FROM zone_exposed.observatory_perimeters
          WHERE year = (SELECT max(year) FROM zone_exposed.observatory_perimeters)
            AND {type_} = %(code)s
        )""")
        params["code"] = code
    sql = f"""
      SELECT id_lieu, nom_lieu, com_lieu, type, date_maj,
             nbre_pl, nbre_pmr, duree, horaires, proprio, lumiere, geom
      FROM zone_exposed.aires_covoiturage
      WHERE {' AND '.join(where)}
    """
    return sql, params
