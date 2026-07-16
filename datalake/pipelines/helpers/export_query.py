"""Per-target export query builder.

Ported from the API's `config/export.ts` (fields + filters); the geo/operator
SQL builders formerly in the API's `ExportParams` now live here as the canonical
implementation. The datalake worker uses these pure functions to build the
`COPY (SELECT ...) TO STDOUT` inner query against `zone_exposed.export_partners`.

Geo codes and dates originate from a user's export request, so every
interpolated value is validated against a strict allowlist / regex before it
reaches the SQL string (guards against SQL injection).
"""

import re

# Territory selector keys we accept from the request (allowlist).
ALLOWED_GEO_KEYS = {"arr", "com", "dep", "epci", "aom", "reg", "country", "insee"}
# Geo codes are short alphanumerics (INSEE, EPCI SIREN, etc.).
_CODE_RE = re.compile(r"^[A-Za-z0-9]{1,15}$")
# Dates are already normalized to YYYY-MM-DD by the worker.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Port of config/export.ts `fields` (order is the CSV column order).
FIELDS = [
    "journey_id", "operator_trip_id", "operator_journey_id", "operator_class",
    "status",
    "start_datetime", "start_date", "start_time",
    "end_datetime", "end_date", "end_time",
    "duration", "distance",
    "start_lat", "start_lon", "end_lat", "end_lon",
    "start_insee", "start_commune", "start_departement", "start_epci",
    "start_aom", "start_region", "start_pays",
    "end_insee", "end_commune", "end_departement", "end_epci",
    "end_aom", "end_region", "end_pays",
    "operator", "operator_passenger_id", "passenger_identity_key",
    "operator_driver_id", "driver_identity_key",
    "driver_revenue", "passenger_contribution", "passenger_seats",
    "cee_application", "incentive_type", "has_incentive",
    "incentive_0_siret", "incentive_0_name", "incentive_0_amount",
    "incentive_1_siret", "incentive_1_name", "incentive_1_amount",
    "incentive_2_siret", "incentive_2_name", "incentive_2_amount",
    "incentive_rpc_0_campaign_id", "incentive_rpc_0_campaign_name",
    "incentive_rpc_0_siret", "incentive_rpc_0_name", "incentive_rpc_0_amount",
    "incentive_rpc_1_campaign_id", "incentive_rpc_1_campaign_name",
    "incentive_rpc_1_siret", "incentive_rpc_1_name", "incentive_rpc_1_amount",
    "incentive_rpc_2_campaign_id", "incentive_rpc_2_campaign_name",
    "incentive_rpc_2_siret", "incentive_rpc_2_name", "incentive_rpc_2_amount",
]

# Port of config/export.ts `filters` exclusions (live targets only; datagouv
# is a dead enum fallback and is not handled by the worker).
EXCLUSIONS = {
    "operator": ["operator", "has_incentive"],
    "territory": ["has_incentive"],
}

# geo_selector keys -> SQL column suffixes (canonical; formerly ExportParams.geoToSQL).
COLUMN_MAP = {"epci": "epci_code", "aom": "aom_code"}


def select_columns(target: str) -> list[str]:
    excluded = set(EXCLUSIONS.get(target, []))
    return [f for f in FIELDS if f not in excluded]


def operator_to_sql(operator_id: list[int]) -> str:
    if not operator_id:
        return ""
    joined = ",".join(str(int(o)) for o in operator_id)
    return f"AND operator_id IN ({joined})"


def geo_to_sql(geo_selector: dict | None, mode: str = "OR") -> str:
    if not geo_selector:
        return ""
    groups = []
    for key, codes in geo_selector.items():
        if not codes:
            continue
        if key not in ALLOWED_GEO_KEYS:
            raise ValueError(f"unknown geo selector key: {key!r}")
        for c in codes:
            if not _CODE_RE.match(str(c)):
                raise ValueError(f"invalid geo code: {c!r}")
        col = COLUMN_MAP.get(key, key)
        groups.append(" OR ".join(f"start_{col} = '{c}'" for c in codes))
    if not groups:
        return ""
    start = " OR ".join(groups)
    end = start.replace("start_", "end_")
    return f"AND (({start}) {mode} ({end}))"


def build_copy_sql(target: str, params: dict) -> str:
    cols = ", ".join(select_columns(target))
    start_at = str(params["start_at"])
    end_at = str(params["end_at"])
    for d in (start_at, end_at):
        if not _DATE_RE.match(d):
            raise ValueError(f"invalid date (expected YYYY-MM-DD): {d!r}")
    geo = geo_to_sql(params.get("geo_selector"))
    op = operator_to_sql(params.get("operator_id", []))
    return f"""
        SELECT {cols}
        FROM zone_exposed.export_partners
        WHERE start_datetime_tz >= '{start_at}'
          AND start_datetime_tz < '{end_at}'
          {geo}
          {op}
        ORDER BY start_datetime_tz ASC
    """
