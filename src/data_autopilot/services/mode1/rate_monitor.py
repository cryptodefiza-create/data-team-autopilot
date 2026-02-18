from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class RateMonitor:
    """Monitors API calls, LLM usage, and costs per provider/tier.

    Provides an operator dashboard for monitoring system health and costs.
    """

    def __init__(self) -> None:
        self._api_calls: list[dict[str, Any]] = []
        self._llm_usage: list[dict[str, Any]] = []
        self._costs: list[dict[str, Any]] = []

    def record_api_call(
        self,
        provider: str,
        method: str,
        org_id: str = "",
        tier: str = "free",
        latency_ms: float = 0.0,
        success: bool = True,
    ) -> None:
        self._api_calls.append({
            "provider": provider,
            "method": method,
            "org_id": org_id,
            "tier": tier,
            "latency_ms": latency_ms,
            "success": success,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def record_llm_usage(
        self,
        provider: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        task_type: str = "",
        tier: str = "free",
    ) -> None:
        self._llm_usage.append({
            "provider": provider,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost_usd,
            "task_type": task_type,
            "tier": tier,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def get_api_stats(self, provider: str | None = None) -> dict[str, Any]:
        """Get API call statistics, optionally filtered by provider."""
        calls = self._api_calls
        if provider:
            calls = [c for c in calls if c["provider"] == provider]

        total = len(calls)
        successful = sum(1 for c in calls if c["success"])
        avg_latency = (
            sum(c["latency_ms"] for c in calls) / total
            if total > 0 else 0.0
        )

        # Group by provider
        by_provider: dict[str, int] = {}
        for c in calls:
            by_provider[c["provider"]] = by_provider.get(c["provider"], 0) + 1

        return {
            "total_calls": total,
            "successful": successful,
            "error_rate": (total - successful) / total if total > 0 else 0.0,
            "avg_latency_ms": avg_latency,
            "by_provider": by_provider,
        }

    def get_llm_stats(self) -> dict[str, Any]:
        """Get LLM usage statistics."""
        total_cost = sum(u["cost_usd"] for u in self._llm_usage)
        total_tokens = sum(u["tokens_in"] + u["tokens_out"] for u in self._llm_usage)

        by_tier: dict[str, float] = {}
        for u in self._llm_usage:
            tier = u["tier"]
            by_tier[tier] = by_tier.get(tier, 0.0) + u["cost_usd"]

        return {
            "total_queries": len(self._llm_usage),
            "total_cost_usd": total_cost,
            "total_tokens": total_tokens,
            "cost_per_query": total_cost / len(self._llm_usage) if self._llm_usage else 0.0,
            "by_tier": by_tier,
        }

    def get_cost_summary(self) -> dict[str, Any]:
        """Get overall cost summary across all services."""
        api_stats = self.get_api_stats()
        llm_stats = self.get_llm_stats()

        return {
            "api_calls": api_stats["total_calls"],
            "llm_queries": llm_stats["total_queries"],
            "llm_cost_usd": llm_stats["total_cost_usd"],
            "cost_per_query_avg": llm_stats["cost_per_query"],
        }
