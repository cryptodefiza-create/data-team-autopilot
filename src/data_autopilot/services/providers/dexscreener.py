from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import PoolReport, ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.dexscreener.com/latest"


class DexScreenerProvider(BaseProvider):
    """DEX pair data: price, volume, liquidity, pair address."""

    name = "dexscreener"

    def __init__(self, api_key: str = "", base_url: str = _BASE_URL,
                 mock_mode: bool = False) -> None:
        super().__init__(api_key=api_key, base_url=base_url)
        self._mock_mode = mock_mode
        self._mock_pools: dict[str, dict[str, Any]] = {}

    def register_mock_pool(self, address: str, data: dict[str, Any]) -> None:
        self._mock_pools[address] = data

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_pair": self._get_pair,
            "get_price": self._get_price,
            "search_pairs": self._search_pairs,
        })

    def _get_pair(self, params: dict[str, Any]) -> ProviderResult:
        chain = params.get("chain", "ethereum")
        pair_address = params.get("address", "")
        if not pair_address:
            return ProviderResult(
                provider=self.name, method="get_pair", error="pair address required"
            )
        if self._mock_mode:
            mock = self._mock_pools.get(pair_address, {
                "pairAddress": pair_address,
                "baseToken": {"symbol": "BONK", "name": "Bonk"},
                "quoteToken": {"symbol": "SOL", "name": "Solana"},
                "liquidity": {"usd": 5_000_000},
                "volume": {"h24": 2_500_000},
                "priceUsd": "0.0000234",
                "fdv": 1_500_000_000,
                "chainId": chain,
                "dexId": "raydium",
            })
            return ProviderResult(
                provider=self.name, method="get_pair",
                records=[{
                    "pair_address": mock.get("pairAddress", pair_address),
                    "base_token": mock.get("baseToken", {}).get("symbol", ""),
                    "quote_token": mock.get("quoteToken", {}).get("symbol", ""),
                    "price_usd": mock.get("priceUsd", ""),
                    "volume_24h": mock.get("volume", {}).get("h24", 0),
                    "liquidity_usd": mock.get("liquidity", {}).get("usd", 0),
                    "dex": mock.get("dexId", ""),
                }],
                total_available=1,
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
        if self._mock_mode:
            return ProviderResult(
                provider=self.name, method="search_pairs",
                records=[{
                    "pair_address": "mock_pair_address",
                    "base_token": query.upper(),
                    "quote_token": "SOL",
                    "price_usd": "1.50",
                    "volume_24h": 500_000,
                    "liquidity_usd": 1_000_000,
                    "chain": "solana",
                    "dex": "raydium",
                }],
                total_available=1,
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

    def get_pool_report(self, address: str, protocol: str = "raydium",
                        chain: str = "solana") -> PoolReport:
        """Get a structured pool analytics report."""
        result = self.fetch("get_pair", {"address": address, "chain": chain})
        if result.error or not result.records:
            return PoolReport(pool_address=address, protocol=protocol)
        pair = result.records[0]
        tvl = float(pair.get("liquidity_usd", 0))
        vol_24h = float(pair.get("volume_24h", 0))
        fees_24h = vol_24h * 0.003  # Standard 0.3% fee
        fee_apr = (fees_24h * 365 / tvl * 100) if tvl > 0 else 0.0

        return PoolReport(
            pool_address=address,
            protocol=protocol,
            tvl=tvl,
            volume_24h=vol_24h,
            fees_24h=fees_24h,
            fee_apr=fee_apr,
            token_0=str(pair.get("base_token", "")),
            token_1=str(pair.get("quote_token", "")),
        )
