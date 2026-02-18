from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.llama.fi"


class DefiLlamaProvider(BaseProvider):
    """Protocol TVL, fees, revenue, chain-level metrics."""

    name = "defillama"

    def __init__(self, api_key: str = "", base_url: str = _BASE_URL) -> None:
        super().__init__(api_key=api_key, base_url=base_url)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        dispatch = {
            "get_tvl": self._get_tvl,
            "get_protocol": self._get_protocol,
            "get_chain_tvl": self._get_chain_tvl,
        }
        handler = dispatch.get(method)
        if handler is None:
            return ProviderResult(
                provider=self.name, method=method, error=f"Unknown method: {method}"
            )
        try:
            return handler(params)
        except Exception as exc:
            logger.error("DefiLlama %s failed: %s", method, exc, exc_info=True)
            return ProviderResult(provider=self.name, method=method, error=str(exc))

    def _get_tvl(self, params: dict[str, Any]) -> ProviderResult:
        protocol = params.get("protocol", params.get("token", ""))
        if not protocol:
            return ProviderResult(
                provider=self.name, method="get_tvl", error="protocol name required"
            )
        slug = protocol.lower().strip().replace(" ", "-")
        data = self._get(f"{self.base_url}/protocol/{slug}")
        if isinstance(data, dict) and "tvl" in data:
            tvl_history = data.get("tvl", [])
            records: list[dict[str, Any]]
            if isinstance(tvl_history, list):
                records = [
                    {"date": entry.get("date", ""), "tvl_usd": entry.get("totalLiquidityUSD", 0)}
                    for entry in tvl_history[-90:]  # last 90 data points
                ]
            else:
                records = [{"protocol": slug, "current_tvl": data.get("currentChainTvls", {})}]
            return ProviderResult(
                provider=self.name,
                method="get_tvl",
                records=records,
                total_available=len(records),
            )
        return ProviderResult(
            provider=self.name, method="get_tvl", error=f"No data for {slug}"
        )

    def _get_protocol(self, params: dict[str, Any]) -> ProviderResult:
        protocol = params.get("protocol", params.get("token", ""))
        if not protocol:
            return ProviderResult(
                provider=self.name, method="get_protocol", error="protocol name required"
            )
        slug = protocol.lower().strip().replace(" ", "-")
        data = self._get(f"{self.base_url}/protocol/{slug}")
        if isinstance(data, dict) and "name" in data:
            record = {
                "name": data.get("name"),
                "symbol": data.get("symbol"),
                "category": data.get("category"),
                "chains": data.get("chains", []),
                "tvl": data.get("currentChainTvls", {}),
                "url": data.get("url"),
            }
            return ProviderResult(
                provider=self.name,
                method="get_protocol",
                records=[record],
                total_available=1,
            )
        return ProviderResult(
            provider=self.name, method="get_protocol", error=f"No protocol: {slug}"
        )

    def _get_chain_tvl(self, params: dict[str, Any]) -> ProviderResult:
        chain = params.get("chain", "")
        if not chain:
            # Return all chains summary
            data = self._get(f"{self.base_url}/v2/chains")
            if isinstance(data, list):
                records = [
                    {"name": c.get("name", ""), "tvl": c.get("tvl", 0)}
                    for c in data[:50]
                ]
                return ProviderResult(
                    provider=self.name,
                    method="get_chain_tvl",
                    records=records,
                    total_available=len(data),
                )
        else:
            data = self._get(f"{self.base_url}/v2/historicalChainTvl/{chain}")
            if isinstance(data, list):
                records = [
                    {"date": entry.get("date", ""), "tvl": entry.get("tvl", 0)}
                    for entry in data[-90:]
                ]
                return ProviderResult(
                    provider=self.name,
                    method="get_chain_tvl",
                    records=records,
                    total_available=len(data),
                )
        return ProviderResult(
            provider=self.name, method="get_chain_tvl", error="No chain TVL data"
        )
