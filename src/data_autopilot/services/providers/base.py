from __future__ import annotations

import abc
import logging
from typing import Any

import httpx

from data_autopilot.services.mode1.models import ProviderResult

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15.0
_MAX_RETRIES = 2


class BaseProvider(abc.ABC):
    """Abstract base for blockchain data providers."""

    name: str = "base"

    def __init__(self, api_key: str = "", base_url: str = "") -> None:
        self.api_key = api_key
        self.base_url = base_url
        self._client = httpx.Client(timeout=_DEFAULT_TIMEOUT)

    # Subclasses implement this
    @abc.abstractmethod
    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult: ...

    def _post_json_rpc(self, url: str, method: str, params: list | dict) -> dict:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._client.post(url, json=payload)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning(
                    "JSON-RPC %s attempt %d failed: %s", method, attempt + 1, exc
                )
                if attempt == _MAX_RETRIES:
                    raise
        return {}  # unreachable

    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict | list:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning(
                    "GET %s attempt %d failed: %s", url, attempt + 1, exc
                )
                if attempt == _MAX_RETRIES:
                    raise
        return {}  # unreachable

    def close(self) -> None:
        self._client.close()
