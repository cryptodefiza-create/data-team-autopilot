from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.dexscreener.com/latest"


class DexScreenerProvider(BaseProvider):
    """DEX pair data: price, volume, liquidity, pair address."""

    name = "dexscreener"

    def __init__(self, api_key: str = "", base_url: str = _BASE_URL) -> None:
        super().__init__(api_key=api_key, base_url=base_url)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        dispatch = {
            "get_pair": self._get_pair,
            "get_price": self._get_price,
            "search_pairs": self._search_pairs,
        }
        handler = dispatch.get(method)
        if handler is None:
            return ProviderResult(
                provider=self.name, method=method, error=f"Unknown method: {method}"
            )
        try:
            return handler(params)
        except Exception as exc:
            logger.error("DexScreener %s failed: %s", method, exc, exc_info=True)
            return ProviderResult(provider=self.name, method=method, error=str(exc))

    def _get_pair(self, params: dict[str, Any]) -> ProviderResult:
        chain = params.get("chain", "ethereum")
        pair_address = params.get("address", "")
        if not pair_address:
            return ProviderResult(
                provider=self.name, method="get_pair", error="pair address required"
            )
        data = self._get(f"{self.base_url}/dex/pairs/{chain}/{pair_address}")
        pairs = data.get("pairs", []) if isinstance(data, dict) else []
        records = [
            {
                "pair_address": p.get("pairAddress", ""),
                "base_token": p.get("baseToken", {}).get("symbol", ""),
                "quote_token": p.get("quoteToken", {}).get("symbol", ""),
                "price_usd": p.get("priceUsd", ""),
                "volume_24h": p.get("volume", {}).get("h24", 0),
                "liquidity_usd": p.get("liquidity", {}).get("usd", 0),
                "dex": p.get("dexId", ""),
            }
            for p in pairs
        ]
        return ProviderResult(
            provider=self.name,
            method="get_pair",
            records=records,
            total_available=len(records),
        )

    def _get_price(self, params: dict[str, Any]) -> ProviderResult:
        """Fallback price lookup via token search."""
        token = params.get("token", "")
        if not token:
            return ProviderResult(
                provider=self.name, method="get_price", error="token required"
            )
        return self._search_pairs({"query": token})

    def _search_pairs(self, params: dict[str, Any]) -> ProviderResult:
        query = params.get("query", params.get("token", ""))
        if not query:
            return ProviderResult(
                provider=self.name, method="search_pairs", error="query required"
            )
        data = self._get(f"{self.base_url}/dex/search", params={"q": query})
        pairs = data.get("pairs", []) if isinstance(data, dict) else []
        records = [
            {
                "pair_address": p.get("pairAddress", ""),
                "base_token": p.get("baseToken", {}).get("symbol", ""),
                "quote_token": p.get("quoteToken", {}).get("symbol", ""),
                "price_usd": p.get("priceUsd", ""),
                "volume_24h": p.get("volume", {}).get("h24", 0),
                "liquidity_usd": p.get("liquidity", {}).get("usd", 0),
                "chain": p.get("chainId", ""),
                "dex": p.get("dexId", ""),
            }
            for p in pairs[:20]  # cap to top 20 results
        ]
        return ProviderResult(
            provider=self.name,
            method="search_pairs",
            records=records,
            total_available=len(pairs),
        )
