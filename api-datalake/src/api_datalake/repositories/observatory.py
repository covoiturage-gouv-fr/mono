"""Requêtes de lecture de l'observatoire sur `zone_exposed`.

Porté depuis `api/src/pdc/services/observatory/providers/*`. Toutes les valeurs
sont liées (paramétrées) ; `type` est en plus validé par allowlist en amont.
"""

from ..helpers import check_territory_param

# Toutes les sources sont dans la zone exposée : l'API ne lit rien d'autre.
LOCATION_TABLE = "zone_exposed.location"      # remplace observatoire_stats.view_location
PERIMETERS_TABLE = "zone_exposed.observatory_perimeters"  # remplace geo.perimeters
CAMPAIGNS_TABLE = "zone_exposed.campaigns"    # remplace raw_zone.campaigns + jointure geom

# Contrat public de /observatory/campaigns : colonnes projetées explicitement.
# `SELECT *` serait fragile — toute colonne ajoutée à la vue fuiterait sur l'API
# publique. Un test fige cet ensemble (test_hardening).
CAMPAIGNS_COLUMNS = (
    "type", "code", "premiere_campagne", "budget_incitations", "date_debut",
    "date_fin", "conducteur_montant_max_par_passager",
    "conducteur_montant_max_par_mois", "conducteur_montant_min_par_passager",
    "conducteur_trajets_max_par_mois", "passager_trajets_max_par_mois",
    "passager_gratuite", "passager_eligible_gratuite", "passager_reduction_ticket",
    "passager_eligibilite_reduction", "passager_montant_ticket",
    "zone_sens_des_trajets", "zone_exclusion", "si_zone_exclue_liste",
    "autre_exclusion", "trajet_longueur_min", "trajet_longueur_max",
    "trajet_classe_de_preuve", "operateurs", "autres_informations", "lien", "geom",
)

# type de territoire -> colonne de périmètre. `com` = grain arrondissement (`arr`),
# comme l'ancienne requête de l'API (SELECT arr AS com ...).
_PERIM_COL = {
    "com": "arr", "epci": "epci", "aom": "aom",
    "dep": "dep", "reg": "reg", "country": "country",
}


def build_location_query(type_: str, code: str, year: int, n: int,
                         month: int | None = None, trimester: int | None = None,
                         semester: int | None = None) -> tuple[str, dict]:
    """Construit la requête de heatmap (binning H3 en SQL) + ses paramètres.

    Porté de `LocationRepositoryProvider` : résolution du périmètre au millésime,
    filtre temporel, puis binning des points de départ ET d'arrivée à la résolution
    `n` (h3_cell_to_parent sur l'index z8 pré-calculé) et comptage par hexagone.
    """
    type_ = check_territory_param(type_)
    col = _PERIM_COL[type_]

    params: dict = {"code": code, "year": year, "n": n}
    period = ["EXTRACT(YEAR FROM start_datetime) = %(year)s"]
    if month is not None:
        period.append("EXTRACT(MONTH FROM start_datetime) = %(month)s")
        params["month"] = month
    if trimester is not None:
        period.append("EXTRACT(QUARTER FROM start_datetime) = %(trimester)s")
        params["trimester"] = trimester
    if semester is not None:
        # Réplique à l'identique la logique de l'API (Q4 -> S2, sinon S1).
        period.append(
            "(CASE WHEN EXTRACT(QUARTER FROM start_datetime)::int > 3 THEN 2 ELSE 1 END) = %(semester)s"
        )
        params["semester"] = semester
    period_sql = " AND ".join(period)

    geo = ("(start_geo_code IN (SELECT com FROM perims) "
           "OR end_geo_code IN (SELECT com FROM perims))")

    sql = f"""
        WITH millesime AS (
            SELECT year FROM (
                SELECT max(year) AS year FROM {PERIMETERS_TABLE} WHERE year = %(year)s
                UNION ALL
                SELECT max(year) AS year FROM {PERIMETERS_TABLE}
                ORDER BY year
                LIMIT 1
            ) m
        ),
        perims AS (
            SELECT arr AS com
            FROM {PERIMETERS_TABLE}
            WHERE year = (SELECT year FROM millesime)
              AND {col} = %(code)s
        ),
        pts AS (
            SELECT h3_cell_to_parent(start_h3index_z8, %(n)s) AS hex
            FROM {LOCATION_TABLE}
            WHERE {period_sql} AND {geo}
            UNION ALL
            SELECT h3_cell_to_parent(end_h3index_z8, %(n)s) AS hex
            FROM {LOCATION_TABLE}
            WHERE {period_sql} AND {geo}
        )
        SELECT hex::text AS hex, count(*)::int AS count
        FROM pts
        GROUP BY hex
    """
    return sql, params


async def get_location(conn, type_: str, code: str, year: int, n: int,
                       month: int | None = None, trimester: int | None = None,
                       semester: int | None = None) -> list[dict]:
    sql, params = build_location_query(type_, code, year, n, month, trimester, semester)
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()
    return [{"hex": r["hex"], "count": r["count"]} for r in rows]


def build_campaigns_query(type_: str | None = None, code: str | None = None,
                          year: int | None = None) -> tuple[str, dict]:
    """Construit la requête des campagnes d'incitation + ses paramètres.

    Porté de `IncentiveCampaignsRepositoryProvider` : la géométrie et la jointure
    périmètre sont déjà matérialisées dans `zone_exposed.campaigns` ; il reste les
    filtres temporels (dépendants de `now()`) et le filtrage type/code/année.
    """
    filters = ["geom IS NOT NULL"]
    params: dict = {}
    if code:
        filters.append("left(code, 9) = %(code)s")
        params["code"] = code
    if year and not code:
        filters.append("EXTRACT(YEAR FROM date_fin) = %(year)s")
        filters.append("date_fin < now()")
        params["year"] = year
    if year and code:
        filters.append("EXTRACT(YEAR FROM date_fin) = %(year)s")
        params["year"] = year
    if not year and not code:
        filters.append("date_fin > now()")
    if type_ is not None:
        filters.append("type = %(type)s")
        params["type"] = check_territory_param(type_)

    # Projection explicite (pas de SELECT *) : défense en profondeur si une colonne
    # est ajoutée à la vue, elle ne fuite pas automatiquement sur l'API publique.
    cols = ", ".join(CAMPAIGNS_COLUMNS)
    sql = f"SELECT {cols} FROM {CAMPAIGNS_TABLE} WHERE " + " AND ".join(filters)
    return sql, params


async def get_campaigns(conn, type_: str | None = None, code: str | None = None,
                        year: int | None = None) -> list[dict]:
    sql, params = build_campaigns_query(type_, code, year)
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        rows = await cur.fetchall()
    return list(rows)


async def get_last_record(conn, type_: str, code: str,
                          max_ym: int | None = None) -> dict | None:
    """Dernier (year, month) disponible pour un territoire, cutoff optionnel.

    Source : `zone_exposed.od_month` (ex `observatoire_stats.flux_by_month`).
    """
    type_ = check_territory_param(type_)
    sql = [
        "SELECT year, month",
        "FROM zone_exposed.od_month",
        "WHERE type = %(type)s",
        "  AND (territory_1 = %(code)s OR territory_2 = %(code)s)",
    ]
    params: dict = {"type": type_, "code": code}
    if max_ym is not None:
        sql.append("  AND (year * 100 + month) <= %(max_ym)s")
        params["max_ym"] = max_ym
    sql.append("ORDER BY year DESC, month DESC")
    sql.append("LIMIT 1")

    async with conn.cursor() as cur:
        await cur.execute("\n".join(sql), params)
        row = await cur.fetchone()
    if not row:
        return None
    return {"year": row["year"], "month": row["month"]}
