from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult, RevenueReport
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.llama.fi"
_FEES_URL = "https://fees.llama.fi"


class DefiLlamaProvider(BaseProvider):
    """Protocol TVL, fees, revenue, chain-level metrics."""

    name = "defillama"

    def __init__(self, api_key: str = "", base_url: str = _BASE_URL,
                 mock_mode: bool = False) -> None:
        super().__init__(api_key=api_key, base_url=base_url)
        self._mock_mode = mock_mode
        self._mock_data: dict[str, Any] = {}

    def register_mock_data(self, method: str, data: Any) -> None:
        self._mock_data[method] = data

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_tvl": self._get_tvl,
            "get_protocol": self._get_protocol,
            "get_chain_tvl": self._get_chain_tvl,
            "get_fees": self._get_fees,
        })

    def _get_tvl(self, params: dict[str, Any]) -> ProviderResult:
        protocol = params.get("protocol", params.get("token", ""))
        if not protocol:
            return ProviderResult(
                provider=self.name, method="get_tvl", error="protocol name required"
            )
        slug = protocol.lower().strip().replace(" ", "-")
        if self._mock_mode:
            data = self._mock_data.get("get_tvl", {
                "name": protocol,
                "tvl": 12_500_000_000,
                "currentChainTvls": {
                    "Ethereum": 8_000_000_000,
                    "Polygon": 2_500_000_000,
                    "Arbitrum": 2_000_000_000,
                },
            })
            return ProviderResult(
                provider=self.name, method="get_tvl",
                records=[data], total_available=1,
            )
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

    def _get_fees(self, params: dict[str, Any]) -> ProviderResult:
        protocol = params.get("protocol", "")
        if not protocol:
            return ProviderResult(
                provider=self.name, method="get_fees", error="protocol name required"
            )
        if self._mock_mode:
            daily = [
                {"date": f"2025-03-{d:02d}", "fees": 150_000 + d * 5_000,
                 "revenue": 45_000 + d * 1_500}
                for d in range(1, 31)
            ]
            data = self._mock_data.get("get_fees", {
                "name": protocol,
                "total_fees": sum(d["fees"] for d in daily),
                "protocol_revenue": sum(d["revenue"] for d in daily),
                "daily": daily,
            })
            return ProviderResult(
                provider=self.name, method="get_fees",
                records=[data], total_available=1,
            )
        slug = protocol.lower().strip().replace(" ", "-")
        data = self._get(f"{_FEES_URL}/overview/fees/{slug}")
        return ProviderResult(
            provider=self.name, method="get_fees",
            records=[data] if isinstance(data, dict) else [],
            total_available=1,
        )

    def get_revenue_report(self, protocol_name: str, days: int = 30) -> RevenueReport:
        """Get a structured revenue report for a protocol."""
        result = self.fetch("get_fees", {"protocol": protocol_name})
        if result.error or not result.records:
            return RevenueReport(protocol_name=protocol_name, period_days=days)
        data = result.records[0]
        daily = data.get("daily", [])
        total_fees = float(data.get("total_fees", 0))
        protocol_revenue = float(data.get("protocol_revenue", 0))

        trend = "stable"
        if len(daily) >= 7:
            first_half = sum(d.get("fees", 0) for d in daily[: len(daily) // 2])
            second_half = sum(d.get("fees", 0) for d in daily[len(daily) // 2 :])
            if second_half > first_half * 1.1:
                trend = "increasing"
            elif second_half < first_half * 0.9:
                trend = "decreasing"

        return RevenueReport(
            protocol_name=protocol_name,
            total_fees=total_fees,
            protocol_revenue=protocol_revenue,
            daily_breakdown=daily,
            trend=trend,
            period_days=days,
        )
