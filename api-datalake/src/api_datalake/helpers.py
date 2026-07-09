"""Validation des paramètres partagés de l'observatoire."""

# Allowlist des types de territoire (défense en profondeur : `type` peut être
# interpolé dans des noms de colonnes selon l'endpoint). Fallback = "com".
PERIMETER_TYPES = ("com", "epci", "aom", "dep", "reg", "country")


def check_territory_param(territory: str | None) -> str:
    return territory if territory in PERIMETER_TYPES else "com"
