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
import logging

from .config import settings

logger = logging.getLogger("api_datalake")

_VERSION_KEY = "obs:cache:version"
_client = None


def open_redis():
    """Ouvre le client Redis si `REDIS_URL` est configuré, sinon None (cache off).

    Pour un URL `rediss://`, on vérifie le certificat TLS contre la CA privée
    fournie par `REDIS_CA` (PEM en mémoire, aucun montage de fichier). La
    vérification n'est **jamais** désactivée. Les paramètres TLS ne sont passés
    que pour `rediss://` (sinon `from_url` refuse des « SSL arguments » sur une
    connexion non chiffrée).
    """
    global _client
    if _client is None and settings.redis_url:
        import redis.asyncio as aioredis
        kwargs = {}
        if settings.redis_url.startswith("rediss://") and settings.redis_ca:
            kwargs["ssl_ca_data"] = settings.redis_ca
        _client = aioredis.from_url(settings.redis_url, **kwargs)
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
    cutoff ne bouge pas. Absent ou Redis en panne -> `0` (jamais d'exception).
    """
    counter = None
    if redis is not None:
        try:
            counter = await redis.get(_VERSION_KEY)
        except Exception:
            logger.warning("cache version read failed", exc_info=True)
    counter = counter.decode() if isinstance(counter, bytes) else (counter or "0")
    return f"{cutoff or 'none'}.{counter}"


async def cache_get(redis, key):
    """Renvoie `(valeur|None, ok)`. `ok=False` si Redis est en panne.

    Une panne (TLS, réseau, indispo) ne lève jamais : on sert alors depuis PG.
    Cache désactivé (redis/clé absents) n'est pas une panne -> `ok=True`.
    """
    if redis is None or key is None:
        return None, True
    try:
        return await redis.get(key), True
    except Exception:
        logger.warning("cache read failed, serving uncached", exc_info=True)
        return None, False


async def cache_set(redis, key, blob, ttl) -> None:
    """Écrit dans le cache sans jamais lever (best-effort)."""
    if redis is None or key is None:
        return
    try:
        await redis.set(key, blob, ex=ttl)
    except Exception:
        logger.warning("cache write failed", exc_info=True)
