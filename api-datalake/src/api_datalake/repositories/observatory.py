"""Requêtes de lecture de l'observatoire sur `zone_exposed`.

Porté depuis `api/src/pdc/services/observatory/providers/*`. Toutes les valeurs
sont liées (paramétrées) ; `type` est en plus validé par allowlist en amont.
"""

from ..helpers import check_territory_param

# Toutes les sources sont dans la zone exposée : l'API ne lit rien d'autre.
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
    "trajet_classe_de_preuve", "operateurs", "autres_informations", "lien", "libelle", "geom",
)

# Grain de période -> (suffixe de table exposée, colonne de filtre, valeur). Un seul
# grain à la fois ; sans grain -> année. Les valeurs sont figées (pas d'entrée
# utilisateur) : interpolables sans risque dans le nom de table / la colonne.
def _grain(month: int | None, trimester: int | None,
           semester: int | None) -> tuple[str, str | None, int | None]:
    if month is not None:
        return "month", "month", month
    if trimester is not None:
        return "quarter", "quarter", trimester
    if semester is not None:
        return "semester", "semester", semester
    return "year", None, None


def build_location_query(type_: str, code: str, year: int, n: int,
                         month: int | None = None, trimester: int | None = None,
                         semester: int | None = None) -> tuple[str, dict]:
    """Construit la requête de heatmap (histogramme H3) + ses paramètres.

    Lecture directe de l'agrégat exposé `zone_exposed.location_<grain>` (pré-agrégé
    par territoire / période / hexagone z8), filtré sur (type, code, période). Le
    binning au zoom demandé se fait via `h3_cell_to_parent(hex_z8, n)` sur ce petit
    ensemble — plus aucun scan de `carpools` à la volée.
    """
    type_ = check_territory_param(type_)
    grain, grain_col, grain_val = _grain(month, trimester, semester)

    params: dict = {"type": type_, "code": code, "year": year, "n": n}
    where = ["type = %(type)s", "code = %(code)s", "year = %(year)s"]
    if grain_col is not None:
        where.append(f"{grain_col} = %(grain_val)s")
        params["grain_val"] = grain_val

    sql = f"""
        SELECT h3_cell_to_parent(hex_z8, %(n)s)::text AS hex,
               sum(count)::int AS count
        FROM zone_exposed.location_{grain}
        WHERE {" AND ".join(where)}
        GROUP BY 1
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


# --------------------------------------------------------------------------- #
# Recherche de territoires (autocomplete) — remplace l'index Meilisearch `geo`.
# Source : zone_exposed.observatory_search_territories (modèle dbt + index GIN trgm).
# --------------------------------------------------------------------------- #

SEARCH_TABLE = "zone_exposed.observatory_search_territories"
SEARCH_COLUMNS = ("id", "territory", "l_territory", "type", "year")

# En dessous de 3 caractères, `q` n'a aucun trigramme : l'index GIN est inopérant
# et un `LIKE '%q%'` dégénère en seq scan. On ne garde alors que la résolution
# exacte (id / code de territoire), qui reste servie par un index btree.
SEARCH_MIN_FUZZY_LEN = 3

# `%` et `_` sont des jokers LIKE. On les neutralise (ainsi que `\`) pour que `q`
# reste une sous-chaîne littérale — sinon `q="_"` matche tout et force un scan
# large. Le `ESCAPE '\'` explicite dans la requête fige le caractère d'échappement.
_LIKE_METACHARS = str.maketrans({"\\": r"\\", "%": r"\%", "_": r"\_"})


def build_territory_search_query(q: str, limit: int = 20,
                                 year: int | None = None) -> tuple[str, dict]:
    """Requête d'autocomplete de territoires + résolution exacte d'un `id`.

    - `q` >= 3 caractères : sous-chaîne désaccentuée (`LIKE`) OU proximité
      trigramme (`%`), toutes deux accélérées par l'index GIN du modèle ;
    - `q` plus court : résolution exacte seulement, pour éviter un seq scan sur
      un motif non indexable ;
    - dans tous les cas, égalité `id` / `territory` — servie par les index btree
      du modèle et casse préservée (le front résout par l'`id` stocké tel quel).

    Millésime : `year` explicite, sinon le dernier (`is_latest`). Valeurs liées
    (`%(...)s`) ; `%%` dans la chaîne = littéral.
    """
    q = q.strip()
    params: dict = {"q_exact": q, "q": q.lower(), "limit": limit}

    year_clause = "is_latest"
    if year is not None:
        year_clause = "year = %(year)s"
        params["year"] = year

    exact = "(id = %(q_exact)s OR territory = %(q_exact)s)"
    where_or = [exact]
    order_by = [f"{exact} DESC"]
    if len(q) >= SEARCH_MIN_FUZZY_LEN:
        params["q_like"] = f"%{q.lower().translate(_LIKE_METACHARS)}%"
        where_or[:0] = [
            "public.immutable_unaccent(lower(l_territory)) "
            "LIKE public.immutable_unaccent(%(q_like)s) ESCAPE '\\'",
            "public.immutable_unaccent(lower(l_territory)) "
            "%% public.immutable_unaccent(%(q)s)",
        ]
        order_by.append(
            "similarity(public.immutable_unaccent(lower(l_territory)), "
            "public.immutable_unaccent(%(q)s)) DESC")
    order_by.append("length(l_territory) ASC")

    cols = ", ".join(SEARCH_COLUMNS)
    sql = f"""
        SELECT {cols}
        FROM {SEARCH_TABLE}
        WHERE {year_clause}
          AND ({" OR ".join(where_or)})
        ORDER BY {", ".join(order_by)}
        LIMIT %(limit)s
    """
    return sql, params


async def search_territories(conn, q: str, limit: int = 20,
                             year: int | None = None) -> list[dict]:
    sql, params = build_territory_search_query(q, limit, year)
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        return list(await cur.fetchall())
