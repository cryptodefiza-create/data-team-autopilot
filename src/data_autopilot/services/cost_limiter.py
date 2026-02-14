from __future__ import annotations

import time
from dataclasses import dataclass

from data_autopilot.config.settings import get_settings
from data_autopilot.services.redis_store import RedisStore


@dataclass
class CostBudgetStatus:
    allowed: bool
    bytes_used: int
    bytes_remaining: int
    budget: int
    suggestion: str | None = None


class SlidingWindowCostLimiter:
    def __init__(self, store: RedisStore | None = None) -> None:
        self.settings = get_settings()
        self.store = store or RedisStore(self.settings.redis_url)

    def check(self, org_id: str, estimated_bytes: int) -> CostBudgetStatus:
        key = f"cost_budget:{org_id}"
        now = time.time()
        window_start = now - 3600

        self.store.zremrangebyscore(key, 0, window_start)
        entries = self.store.zrangebyscore(key, window_start, now)
        used = int(sum(v for _, v in entries))
        budget = self.settings.org_hourly_budget_bytes

        if used + estimated_bytes > budget:
            return CostBudgetStatus(
                allowed=False,
                bytes_used=used,
                bytes_remaining=max(0, budget - used),
                budget=budget,
                suggestion="Try sampling or a narrower time window",
            )

        return CostBudgetStatus(
            allowed=True,
            bytes_used=used,
            bytes_remaining=budget - used - estimated_bytes,
            budget=budget,
        )

    def record(self, org_id: str, actual_bytes: int) -> None:
        key = f"cost_budget:{org_id}"
        self.store.zadd(key, score=time.time(), value=float(actual_bytes))
