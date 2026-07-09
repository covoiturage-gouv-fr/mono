"""Configuration par variables d'environnement.

Réutilise les variables `DBT_*` du datalake pour pointer sur la même base
`datalake_production` (lecture des modèles `zone_exposed`).
"""

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_TRUTHY = {"1", "true", "yes", "on"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Mode maintenance : piloté par le ConfigMap (variable MAINTENANCE_MODE).
    maintenance_mode: bool = False

    # Base datalake (mêmes variables que dbt/pipelines)
    dbt_host: str = "localhost"
    dbt_port: int = 5432
    dbt_user: str = "postgres"
    dbt_password: str = ""
    dbt_dbname: str = "datalake"

    # Cache
    redis_url: str | None = None
    cache_ttl_seconds: int = 24 * 3600

    # Fenêtre de publication (borne supérieure exclusive, YYYY-MM-DD)
    app_observatory_published_until: str | None = None

    # CORS (origines autorisées, séparées par des virgules)
    cors_origins: str = "*"

    @field_validator("maintenance_mode", mode="before")
    @classmethod
    def _parse_maintenance(cls, v):
        """Parsing tolérant et insensible à la casse ; tout le reste = inactif."""
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in _TRUTHY

    def conninfo(self) -> str:
        return (
            f"host={self.dbt_host} port={self.dbt_port} "
            f"user={self.dbt_user} password={self.dbt_password} "
            f"dbname={self.dbt_dbname}"
        )


settings = Settings()
