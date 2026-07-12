"""Invalidation du cache de l'API observatoire (api-datalake).

L'API embarque un compteur `obs:cache:version` dans ses clés de cache Redis. Après
chaque (re)construction de la zone exposée, le pipeline incrémente ce compteur : les
anciennes entrées deviennent inatteignables et expirent par TTL.

La clé DOIT rester identique à `api_datalake.cache._VERSION_KEY`.
"""

import os

# Doit correspondre à api_datalake.cache._VERSION_KEY.
VERSION_KEY = "obs:cache:version"


def build_client():
    """Client Redis depuis l'environnement, ou None si `REDIS_URL` absent (cache off).

    Pour un URL `rediss://`, vérifie le TLS contre la CA privée `REDIS_CA` (PEM en
    mémoire) — jamais de vérification désactivée. Symétrique de l'ouverture côté API.
    """
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    import redis

    kwargs = {}
    ca = os.getenv("REDIS_CA")
    if url.startswith("rediss://") and ca:
        kwargs["ssl_ca_data"] = ca
    return redis.from_url(url, **kwargs)


def bump_publication_version(client=None) -> int | None:
    """Incrémente `obs:cache:version`. No-op (renvoie None) si le cache est désactivé.

    `INCR` sur une clé absente part de 0 -> 1 (l'API a `0` par défaut, donc la
    première publication invalide bien le cache).
    """
    client = client or build_client()
    if client is None:
        print("ℹ️ REDIS_URL absent — pas d'invalidation de cache")
        return None
    version = client.incr(VERSION_KEY)
    print(f"✅ cache observatoire invalidé (obs:cache:version = {version})")
    return version
