"""Network client for the official USAspending.gov API."""
from __future__ import annotations

from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.usaspending.gov"


class ApiError(RuntimeError):
    pass


def trusted_download_url(value: str, base_url: str = BASE_URL) -> str:
    absolute = urljoin(base_url, value)
    parsed = urlparse(absolute)
    if parsed.scheme != "https" or parsed.hostname not in {
        "api.usaspending.gov", "files.usaspending.gov"
    }:
        raise ApiError(f"Unsafe download/status URL rejected: {value}")
    return absolute


class UsaSpendingClient:
    def __init__(self, *, timeout: int = 45, session: requests.Session | None = None):
        self.timeout = timeout
        self.session = session or requests.Session()
        if session is None:
            retry = Retry(
                total=5, backoff_factor=1.0,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset({"GET", "POST"}),
                respect_retry_after_header=True,
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("https://", adapter)
        self.session.headers["User-Agent"] = "federal-spending-intelligence/0.1"

    def _request(self, method: str, path_or_url: str, **kwargs):
        url = trusted_download_url(path_or_url)
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            detail = ""
            if getattr(exc, "response", None) is not None:
                detail = f" response={exc.response.text[:400]}"
            raise ApiError(f"{method} {url} failed: {exc}{detail}") from exc

    def post(self, path: str, body: dict) -> dict:
        result = self._request("POST", path, json=body).json()
        if not isinstance(result, dict):
            raise ApiError("Expected JSON object")
        return result

    def get(self, url: str) -> dict:
        result = self._request("GET", url).json()
        if not isinstance(result, dict):
            raise ApiError("Expected JSON object")
        return result

    def stream(self, url: str):
        return self._request("GET", url, stream=True)
