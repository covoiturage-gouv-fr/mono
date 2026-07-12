"""Client data.gouv.fr (port de l'API Deno `DataGouvAPIProvider`, en `requests`).

Choix : porter le client existant plutôt qu'ajouter la lib pré-1.0 `datagouv_client`
— `requests` est déjà une dépendance du datalake, zéro dépendance nouvelle.

Opérations : lire le dataset, uploader une resource (nouvelle, ou remplacer si un même
titre existe déjà — comportement de l'API), poser la description. Auth par `X-API-KEY`.
"""

import os

import requests


class DataGouvClient:
    def __init__(self, base_url: str, api_key: str, dataset: str, session=None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.dataset = dataset
        self._session = session or requests.Session()

    def _headers(self, json_body: bool) -> dict:
        h = {"Accept": "application/json", "X-API-KEY": self.api_key}
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def get_dataset(self) -> dict:
        r = self._session.get(
            f"{self.base_url}/datasets/{self.dataset}",
            headers=self._headers(json_body=False), timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def _find_resource(self, title: str) -> dict | None:
        for res in self.get_dataset().get("resources", []):
            if res.get("title") == title:
                return res
        return None

    def upload(self, filepath: str) -> dict:
        """Upload le fichier comme resource. Remplace si un même titre existe déjà."""
        title = os.path.basename(filepath)
        existing = self._find_resource(title)
        if existing:
            url = f"{self.base_url}/datasets/{self.dataset}/resources/{existing['id']}/upload/"
        else:
            url = f"{self.base_url}/datasets/{self.dataset}/upload/"

        with open(filepath, "rb") as f:
            r = self._session.post(
                url, headers=self._headers(json_body=False),
                files={"file": (title, f)}, timeout=300,
            )
        r.raise_for_status()
        return r.json()

    def set_metadata(self, resource: dict, description: str) -> dict:
        r = self._session.put(
            f"{self.base_url}/datasets/{self.dataset}/resources/{resource['id']}",
            headers=self._headers(json_body=True),
            json={"title": resource.get("title"), "description": description},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
