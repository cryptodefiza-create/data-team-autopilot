from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import PoolReport, RevenueReport
from data_autopilot.services.providers.defillama import DefiLlamaProvider
from data_autopilot.services.providers.dexscreener import DexScreenerProvider
from data_autopilot.services.providers.snapshot_org import SnapshotProvider

logger = logging.getLogger(__name__)


class DeFiAnalytics:
    """Protocol-specific DeFi data: pools, revenue, governance."""

    def __init__(
        self,
        dexscreener: DexScreenerProvider | None = None,
        defillama: DefiLlamaProvider | None = None,
        snapshot: SnapshotProvider | None = None,
    ) -> None:
        self._dexscreener = dexscreener or DexScreenerProvider(mock_mode=True)
        self._defillama = defillama or DefiLlamaProvider(mock_mode=True)
        self._snapshot = snapshot or SnapshotProvider(mock_mode=True)

    def pool_analytics(
        self, pool_address: str, protocol: str = "raydium", chain: str = "solana"
    ) -> PoolReport:
        """Deep pool analysis: liquidity depth, volume, fees, APR."""
        return self._dexscreener.get_pool_report(
            address=pool_address, protocol=protocol, chain=chain,
        )

    def protocol_revenue(
        self, protocol_name: str, days: int = 30
    ) -> RevenueReport:
        """Protocol revenue from DefiLlama fees endpoint."""
        return self._defillama.get_revenue_report(protocol_name, days=days)

    def governance_activity(
        self, protocol_name: str, space: str = ""
    ) -> dict[str, Any]:
        """Snapshot.org governance data."""
        space_id = space or protocol_name.lower().replace(" ", "")
        result = self._snapshot.fetch(
            "get_proposals", {"space": space_id, "limit": 20}
        )
        if result.error:
            return {
                "active_proposals": 0,
                "recent_proposals": [],
                "voter_participation_trend": "unknown",
                "error": result.error,
            }

        proposals = result.records
        active = [p for p in proposals if p.get("state") == "active"]
        closed = [p for p in proposals if p.get("state") == "closed"]

        # Compute participation trend from recent closed proposals
        participation_trend = "stable"
        if len(closed) >= 4:
            first_half_votes = sum(p.get("votes", 0) for p in closed[len(closed) // 2 :])
            second_half_votes = sum(p.get("votes", 0) for p in closed[: len(closed) // 2])
            if second_half_votes > first_half_votes * 1.2:
                participation_trend = "increasing"
            elif second_half_votes < first_half_votes * 0.8:
                participation_trend = "decreasing"

        return {
            "active_proposals": len(active),
            "recent_proposals": proposals[:10],
            "total_proposals": len(proposals),
            "voter_participation_trend": participation_trend,
        }

    def protocol_tvl_breakdown(self, protocol_name: str) -> dict[str, Any]:
        """Get TVL breakdown by chain for a protocol."""
        result = self._defillama.fetch(
            "get_tvl", {"protocol": protocol_name}
        )
        if result.error or not result.records:
            return {"protocol": protocol_name, "tvl": 0, "chains": {}}

        data = result.records[0]
        return {
            "protocol": protocol_name,
            "tvl": data.get("tvl", 0),
            "chains": data.get("currentChainTvls", {}),
        }
