from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MockFailure:
    mode: str
    fail_count: int


class MockQueryExecutor:
    def __init__(self, failure_schedule: dict[str, MockFailure] | None = None) -> None:
        self.failure_schedule = failure_schedule or {}
        self.counts: dict[str, int] = {}

    def execute(self, step_id: str, sql: str) -> dict[str, Any]:
        rule = self.failure_schedule.get(step_id)
        if rule:
            n = self.counts.get(step_id, 0)
            self.counts[step_id] = n + 1
            if n < rule.fail_count:
                raise RuntimeError(rule.mode)

        if "dau" in sql.lower():
            return {
                "rows": [{"day": "2026-02-13", "dau": 12000}, {"day": "2026-02-14", "dau": 12450}],
                "bytes_scanned": 1024 * 1024,
            }

        return {"rows": [{"health_check": 1}], "bytes_scanned": 1024}
