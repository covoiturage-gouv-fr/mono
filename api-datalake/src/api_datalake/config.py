"""Configuration par variables d'environnement.

Réutilise les variables `DBT_*` du datalake pour pointer sur la même base
`datalake_production` (lecture des modèles `zone_exposed`).
"""

from psycopg.conninfo import make_conninfo
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
    # Plafond par requête : borne les scans H3 lourds, protège le pool (ms).
    db_statement_timeout_ms: int = 5000

    # Cache
    redis_url: str | None = None
    redis_ca: str | None = None  # CA privée (PEM) pour Redis TLS ; None en dev clair
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
        # `default_transaction_read_only` : garde-fou en profondeur — même si le rôle
        # PG a des droits d'écriture, aucune requête de l'API ne peut muter la base.
        # `statement_timeout` : coupe les requêtes trop longues avant qu'elles ne
        # saturent le pool. `make_conninfo` échappe correctement chaque valeur
        # (mot de passe avec espace/quote inclus).
        options = (
            f"-c statement_timeout={self.db_statement_timeout_ms} "
            "-c default_transaction_read_only=on"
        )
        return make_conninfo(
            host=self.dbt_host,
            port=self.dbt_port,
            user=self.dbt_user,
            password=self.dbt_password,
            dbname=self.dbt_dbname,
            options=options,
        )


settings = Settings()
