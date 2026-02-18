from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from data_autopilot.services.mode1.models import (
    MartTable,
    Pipeline,
    SemanticContract,
    WeeklyMemo,
)
from data_autopilot.services.mode1.stale_guard import StaleDataGuard

logger = logging.getLogger(__name__)


class WeeklyMemoScheduler:
    """Generates and delivers automated weekly memos from mart data."""

    def __init__(self, stale_guard: StaleDataGuard | None = None) -> None:
        self._guard = stale_guard or StaleDataGuard()
        self._memos: dict[str, list[WeeklyMemo]] = {}  # org_id -> memo history

    def generate_memo(
        self,
        org_id: str,
        pipelines: list[Pipeline],
        marts: dict[str, MartTable],
        contract: SemanticContract | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Generate a weekly memo from mart data.

        Returns dict with status + memo or stale warning.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        # 1. Check data freshness
        freshness = self._guard.check_freshness(pipelines, now=now)
        if not freshness.fresh:
            logger.warning("Memo blocked for org %s: %s", org_id, freshness.message)
            return {
                "status": "blocked",
                "reason": "stale_data",
                "message": freshness.message,
                "stale_pipelines": freshness.stale_pipelines,
            }

        # 2. Compute KPI deltas from marts
        kpis = self._compute_kpis(marts)

        # 3. Generate narrative
        narrative = self._generate_narrative(kpis, contract)

        # 4. Record contract version
        contract_version = contract.version if contract else 0

        # 5. Build memo
        period_end = now
        period_start = now - timedelta(days=7)

        memo = WeeklyMemo(
            org_id=org_id,
            period_start=period_start,
            period_end=period_end,
            kpis=kpis,
            narrative=narrative,
            contract_version=contract_version,
        )

        # Store in history
        if org_id not in self._memos:
            self._memos[org_id] = []
        self._memos[org_id].append(memo)

        logger.info("Generated weekly memo for org %s (contract v%d)", org_id, contract_version)

        return {
            "status": "generated",
            "memo": memo,
        }

    def deliver(
        self,
        memo: WeeklyMemo,
        channels: list[str],
    ) -> list[str]:
        """Deliver memo via configured channels. Returns list of channels delivered to."""
        delivered = []
        for channel in channels:
            # In mock mode, just record the delivery
            memo.delivered_via.append(channel)
            delivered.append(channel)
            logger.info("Delivered memo for org %s via %s", memo.org_id, channel)
        return delivered

    def get_memo_history(self, org_id: str) -> list[WeeklyMemo]:
        return self._memos.get(org_id, [])

    @staticmethod
    def _compute_kpis(marts: dict[str, MartTable]) -> dict[str, Any]:
        """Compute KPI deltas from mart data."""
        kpis: dict[str, Any] = {}

        for mart_name, mart in marts.items():
            if not mart.records:
                continue

            # Find metric columns (prefixed with _)
            metric_cols = [
                col for col in mart.columns
                if col.startswith("_") and col not in ("_ingested_at", "_source")
            ]

            for col in metric_cols:
                values = [
                    float(r.get(col, 0) or 0)
                    for r in mart.records
                    if r.get(col) is not None
                ]
                if values:
                    kpis[col.lstrip("_")] = {
                        "total": sum(values),
                        "count": len(values),
                        "average": sum(values) / len(values),
                    }

            kpis[f"{mart_name}_rows"] = mart.row_count

        return kpis

    @staticmethod
    def _generate_narrative(
        kpis: dict[str, Any],
        contract: SemanticContract | None,
    ) -> str:
        """Generate a narrative summary of KPIs."""
        lines = ["Weekly Data Summary", "=" * 30]

        for kpi_name, kpi_data in kpis.items():
            if isinstance(kpi_data, dict):
                total = kpi_data.get("total", 0)
                count = kpi_data.get("count", 0)
                lines.append(f"- {kpi_name}: total={total:,.2f}, count={count}")
            else:
                lines.append(f"- {kpi_name}: {kpi_data}")

        if contract:
            lines.append(f"\nCalculated using contract v{contract.version}")
            lines.append(f"Timezone: {contract.defaults.timezone}")

        return "\n".join(lines)
