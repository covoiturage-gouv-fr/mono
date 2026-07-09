"""Configuration par variables d'environnement.

Réutilise les variables `DBT_*` du datalake pour pointer sur la même base
`datalake_production` (lecture des modèles `zone_exposed`).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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

    def conninfo(self) -> str:
        return (
            f"host={self.dbt_host} port={self.dbt_port} "
            f"user={self.dbt_user} password={self.dbt_password} "
            f"dbname={self.dbt_dbname}"
        )


settings = Settings()
