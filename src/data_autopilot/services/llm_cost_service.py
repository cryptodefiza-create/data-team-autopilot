"""LLM cost tracking and budget management.

Records per-request token usage with provider/model attribution, computes
estimated costs using configurable per-token rates, and enforces soft budget
caps per org.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.models.entities import AuditLog
from data_autopilot.services.llm_client import LLMResult


# Cost per 1M tokens (USD) — updated Feb 2026 pricing
PROVIDER_RATES: dict[str, dict[str, float]] = {
    "grok-4-fast": {"input": 0.60, "output": 2.40},
    "gpt-5-mini": {"input": 1.50, "output": 6.00},
    "claude-sonnet-4-5-20250929": {"input": 3.00, "output": 15.00},
    # Fallback for unknown models
    "_default": {"input": 1.00, "output": 4.00},
}


def estimate_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate cost in USD for a single LLM call."""
    rates = PROVIDER_RATES.get(model, PROVIDER_RATES["_default"])
    cost = (input_tokens / 1_000_000) * rates["input"] + (output_tokens / 1_000_000) * rates["output"]
    return round(cost, 6)


@dataclass
class LLMUsageRecord:
    tenant_id: str
    provider_name: str
    model: str
    task_type: str
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float
    latency_ms: float
    succeeded: bool
    timestamp: str


@dataclass
class BudgetStatus:
    tenant_id: str
    period: str
    total_cost_usd: float
    budget_usd: float
    remaining_usd: float
    usage_pct: float
    over_budget: bool
    requests: int


class LLMCostService:
    """Tracks LLM token usage and enforces budget soft caps."""

    def record(
        self,
        db: Session,
        *,
        tenant_id: str,
        result: LLMResult,
        task_type: str,
    ) -> LLMUsageRecord:
        """Record a single LLM call's token usage."""
        cost = estimate_cost_usd(result.model, result.input_tokens, result.output_tokens)
        record = LLMUsageRecord(
            tenant_id=tenant_id,
            provider_name=result.provider_name,
            model=result.model,
            task_type=task_type,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            estimated_cost_usd=cost,
            latency_ms=result.latency_ms,
            succeeded=result.succeeded,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        db.add(AuditLog(
            tenant_id=tenant_id,
            event_type="llm_usage",
            payload={
                "provider_name": record.provider_name,
                "model": record.model,
                "task_type": record.task_type,
                "input_tokens": record.input_tokens,
                "output_tokens": record.output_tokens,
                "estimated_cost_usd": record.estimated_cost_usd,
                "latency_ms": record.latency_ms,
                "succeeded": record.succeeded,
            },
        ))
        db.commit()
        return record

    def get_usage_summary(self, db: Session, *, tenant_id: str) -> dict:
        """Aggregate token usage and cost by provider for a tenant."""
        rows = db.execute(
            select(AuditLog)
            .where(AuditLog.tenant_id == tenant_id, AuditLog.event_type == "llm_usage")
            .order_by(AuditLog.created_at.desc())
        ).scalars().all()

        by_provider: dict[str, dict] = {}
        total_cost = 0.0
        total_input = 0
        total_output = 0
        total_requests = 0

        for row in rows:
            p = row.payload or {}
            provider = p.get("provider_name", "unknown")
            if provider not in by_provider:
                by_provider[provider] = {
                    "model": p.get("model", ""),
                    "requests": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "estimated_cost_usd": 0.0,
                    "avg_latency_ms": 0.0,
                    "total_latency_ms": 0.0,
                    "errors": 0,
                }
            entry = by_provider[provider]
            entry["requests"] += 1
            entry["input_tokens"] += int(p.get("input_tokens", 0))
            entry["output_tokens"] += int(p.get("output_tokens", 0))
            entry["estimated_cost_usd"] += float(p.get("estimated_cost_usd", 0))
            entry["total_latency_ms"] += float(p.get("latency_ms", 0))
            if not p.get("succeeded", True):
                entry["errors"] += 1

            total_cost += float(p.get("estimated_cost_usd", 0))
            total_input += int(p.get("input_tokens", 0))
            total_output += int(p.get("output_tokens", 0))
            total_requests += 1

        for entry in by_provider.values():
            if entry["requests"] > 0:
                entry["avg_latency_ms"] = round(entry["total_latency_ms"] / entry["requests"], 2)
            entry["estimated_cost_usd"] = round(entry["estimated_cost_usd"], 6)
            del entry["total_latency_ms"]

        return {
            "tenant_id": tenant_id,
            "total_requests": total_requests,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_estimated_cost_usd": round(total_cost, 6),
            "by_provider": by_provider,
        }

    def get_budget_status(self, db: Session, *, tenant_id: str) -> BudgetStatus:
        """Check current month's LLM spend against budget."""
        settings = get_settings()
        budget = settings.llm_monthly_budget_usd

        usage = self.get_usage_summary(db, tenant_id=tenant_id)
        total_cost = usage["total_estimated_cost_usd"]
        total_requests = usage["total_requests"]

        remaining = max(0.0, budget - total_cost)
        usage_pct = (total_cost / budget * 100) if budget > 0 else 0.0

        return BudgetStatus(
            tenant_id=tenant_id,
            period="all_time",
            total_cost_usd=round(total_cost, 6),
            budget_usd=budget,
            remaining_usd=round(remaining, 6),
            usage_pct=round(usage_pct, 2),
            over_budget=total_cost > budget,
            requests=total_requests,
        )

    def check_budget(self, db: Session, *, tenant_id: str) -> bool:
        """Return True if the org is within budget. Non-blocking soft cap."""
        status = self.get_budget_status(db, tenant_id=tenant_id)
        return not status.over_budget
