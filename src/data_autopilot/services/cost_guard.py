from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BudgetResult:
    allowed: bool
    bytes_used: int
    bytes_remaining: int
    budget: int
    suggestion: str | None = None


class CostGuard:
    def __init__(self, default_budget_bytes: int = 50 * 1024**3) -> None:
        self.default_budget_bytes = default_budget_bytes
        self._usage: dict[str, int] = {}

    def check(self, org_id: str, estimated_bytes: int) -> BudgetResult:
        used = self._usage.get(org_id, 0)
        if used + estimated_bytes > self.default_budget_bytes:
            return BudgetResult(
                allowed=False,
                bytes_used=used,
                bytes_remaining=max(0, self.default_budget_bytes - used),
                budget=self.default_budget_bytes,
                suggestion="Try sampling or narrower time range",
            )
        return BudgetResult(
            allowed=True,
            bytes_used=used,
            bytes_remaining=self.default_budget_bytes - used - estimated_bytes,
            budget=self.default_budget_bytes,
        )

    def record(self, org_id: str, actual_bytes: int) -> None:
        self._usage[org_id] = self._usage.get(org_id, 0) + actual_bytes
