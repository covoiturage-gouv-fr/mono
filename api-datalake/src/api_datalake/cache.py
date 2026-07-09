"""Cache applicatif Redis pour l'observatoire.

Stratégie (reprise du cache historique fait main) : on stocke le **payload déjà
gzippé** et on le sert tel quel en `Content-Encoding: gzip` — zéro recompression
par requête, gros gain sur les flux volumineux.

Invalidation par **version de publication** : la clé de cache embarque une version
(dérivée du cutoff `APP_OBSERVATORY_PUBLISHED_UNTIL` + un compteur `obs:cache:version`
que le pipeline dbt incrémente à chaque publication). Changer de version rend les
anciennes entrées inatteignables ; elles expirent ensuite par TTL.
"""

import gzip
import hashlib
import json

from .config import settings

_VERSION_KEY = "obs:cache:version"
_client = None


def open_redis():
    """Ouvre le client Redis si `REDIS_URL` est configuré, sinon None (cache off)."""
    global _client
    if _client is None and settings.redis_url:
        import redis.asyncio as aioredis
        _client = aioredis.from_url(settings.redis_url)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def get_redis():
    """FastAPI dependency : le client Redis courant (ou None). Surchargeable en test."""
    return _client


def build_cache_key(version: str, route: str, params: dict) -> str:
    """Clé stable, indépendante de l'ordre des params, préfixée par la version."""
    canonical = json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(f"{route}?{canonical}".encode()).hexdigest()[:32]
    return f"obs:{version}:{digest}"


def gzip_payload(data) -> bytes:
    """Sérialise en JSON compact puis gzip. Renvoie les octets à stocker/servir."""
    raw = json.dumps(data, separators=(",", ":"), default=str).encode()
    return gzip.compress(raw, compresslevel=6)


async def publication_version(redis, cutoff: str | None) -> str:
    """Version courante = cutoff + compteur de publication.

    Le compteur permet une invalidation explicite (bump côté pipeline) même si le
    cutoff ne bouge pas. Absent -> `0`.
    """
    counter = await redis.get(_VERSION_KEY) if redis is not None else None
    counter = counter.decode() if isinstance(counter, bytes) else (counter or "0")
    return f"{cutoff or 'none'}.{counter}"
