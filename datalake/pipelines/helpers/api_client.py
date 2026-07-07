"""HTTP client for the export control plane.

Talks to the API's authenticated endpoints (claim/complete/fail). Fetches a
Bearer token via `POST /auth/access_token` and retries on 5xx / connection
errors with exponential backoff.

Response envelope: these endpoints are registered via `apiRoute` without
`rpcAnswerOnSuccess`, so the API returns the raw action result JSON directly
(no `{jsonrpc, result}` wrapper) — hence `claim()` reads `r.json()` as-is.
"""

import time

import requests


class ApiClient:
    def __init__(self, base_url, access_key, secret_key):
        self.base_url = base_url.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self._token = None

    def _auth(self):
        if self._token:
            return self._token
        r = requests.post(
            f"{self.base_url}/auth/access_token",
            json={"access_key": self.access_key, "secret_key": self.secret_key},
            timeout=30,
        )
        r.raise_for_status()
        self._token = r.json()["access_token"]
        return self._token

    def _headers(self):
        return {"Authorization": f"Bearer {self._auth()}"}

    def _post(self, path, body, retries=3):
        for attempt in range(retries):
            try:
                r = requests.post(
                    f"{self.base_url}{path}",
                    json=body,
                    headers=self._headers(),
                    timeout=60,
                )
                if r.status_code >= 500:
                    raise requests.RequestException(f"{r.status_code}")
                return r
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def claim(self, targets):
        r = self._post("/exports/claim", {"targets": targets})
        if r.status_code == 204:
            return None
        return r.json()

    def complete(self, uuid, file_size):
        self._post(f"/exports/{uuid}/complete", {"uuid": uuid, "file_size": file_size})

    def fail(self, uuid, message):
        self._post(f"/exports/{uuid}/fail", {"uuid": uuid, "message": message})
