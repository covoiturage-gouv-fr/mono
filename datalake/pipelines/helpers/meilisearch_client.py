"""Client Meilisearch minimal (REST direct) pour réindexer l'index `geo`.

Réplique `indexData` côté API (`api/src/pdc/services/territory/helpers/meilisearch.ts`,
commande `territory:index`) : purge complète de l'index puis réinsertion par lots.
Même instance/index Meilisearch que l'API — les variables d'env portent volontairement
les mêmes noms (`APP_MEILISEARCH_*`) pour rester le pendant côté datalake de la même
configuration, pas une config parallèle.

Comme côté API, `deleteAllDocuments`/`addDocuments` sont fire-and-forget : Meilisearch
répond avec une tâche mise en file, on n'attend pas sa complétion.
"""

import os

import requests

from pipelines.helpers.retry import retry


def build_config() -> dict | None:
    host = os.getenv("APP_MEILISEARCH_HOST")
    if not host:
        return None
    return {
        "host": host.rstrip("/"),
        "api_key": os.getenv("APP_MEILISEARCH_APIKEY", ""),
        "index": os.getenv("APP_MEILISEARCH_INDEX", "geo"),
        "batch_size": int(os.getenv("APP_MEILISEARCH_BATCH", "1000")),
    }


def _headers(api_key: str) -> dict:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


FILTERABLE_ATTRIBUTES = ["year", "is_latest"]


def index_documents(config: dict, documents: list[dict]) -> int:
    host, index, headers = config["host"], config["index"], _headers(config["api_key"])
    batch_size = config["batch_size"]

    def set_filterable_attributes():
        r = requests.put(
            f"{host}/indexes/{index}/settings/filterable-attributes",
            headers=headers,
            json=FILTERABLE_ATTRIBUTES,
            timeout=30,
        )
        r.raise_for_status()

    retry(set_filterable_attributes, label="configuration filterable-attributes")

    def delete_all():
        r = requests.delete(f"{host}/indexes/{index}/documents", headers=headers, timeout=30)
        r.raise_for_status()

    retry(delete_all, label="purge index geo")

    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]

        def add_batch(batch=batch):
            r = requests.post(f"{host}/indexes/{index}/documents", headers=headers, json=batch, timeout=60)
            r.raise_for_status()

        retry(add_batch, label=f"indexation lot {i // batch_size + 1}")

    return len(documents)
