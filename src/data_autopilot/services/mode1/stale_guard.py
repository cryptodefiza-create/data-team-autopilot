from __future__ import annotations

import logging
from datetime import datetime, timezone

from data_autopilot.services.mode1.models import FreshnessResult, Pipeline, PipelineStatus

logger = logging.getLogger(__name__)

_STALE_THRESHOLD_HOURS = 48.0


class StaleDataGuard:
    """Blocks memo generation if pipeline data is stale (>48 hours)."""

    def __init__(self, threshold_hours: float = _STALE_THRESHOLD_HOURS) -> None:
        self._threshold = threshold_hours

    def check_freshness(
        self,
        pipelines: list[Pipeline],
        now: datetime | None = None,
    ) -> FreshnessResult:
        """Check if all active pipelines have fresh data."""
        if now is None:
            now = datetime.now(timezone.utc)

        if not pipelines:
            return FreshnessResult(
                fresh=True,
                message="No active pipelines to check",
            )

        stale: list[str] = []
        max_hours = 0.0

        for pipeline in pipelines:
            if pipeline.status not in (PipelineStatus.ACTIVE, PipelineStatus.STALE):
                continue

            if pipeline.last_success is None:
                stale.append(pipeline.id)
                continue

            hours_since = (now - pipeline.last_success).total_seconds() / 3600
            max_hours = max(max_hours, hours_since)

            if hours_since > self._threshold:
                stale.append(pipeline.id)

        if stale:
            return FreshnessResult(
                fresh=False,
                stale_pipelines=stale,
                hours_since_last_sync=max_hours,
                message=(
                    f"Data is stale: {len(stale)} pipeline(s) have not synced "
                    f"in over {self._threshold:.0f} hours. "
                    f"Memo generation blocked until data is refreshed."
                ),
            )

        return FreshnessResult(
            fresh=True,
            hours_since_last_sync=max_hours,
            message="All pipelines have fresh data",
        )
