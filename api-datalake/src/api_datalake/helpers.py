"""Validation des paramètres partagés de l'observatoire."""

import re

from fastapi import HTTPException

# Allowlist des types de territoire (défense en profondeur : `type` peut être
# interpolé dans des noms de colonnes selon l'endpoint). Fallback = "com".
PERIMETER_TYPES = ("com", "epci", "aom", "dep", "reg", "country")

# Codes territoire : INSEE commune (dont Corse 2A/2B), SIREN EPCI/AOM, dep, reg.
# Alphanumérique borné — rejette les entrées absurdes (clés de cache infinies,
# scans PG à répétition) avant tout accès base. `fullmatch` : `$` laisserait
# passer un `\n` final.
_CODE_RE = re.compile(r"[0-9A-Za-z]{1,15}")


def check_territory_param(territory: str | None) -> str:
    return territory if territory in PERIMETER_TYPES else "com"


def check_code_param(code: str) -> str:
    if not _CODE_RE.fullmatch(code):
        raise HTTPException(status_code=422, detail="invalid territory code")
    return code
