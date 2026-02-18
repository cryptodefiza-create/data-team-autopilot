from __future__ import annotations

import logging
from datetime import datetime, timezone

from data_autopilot.services.mode1.models import Pipeline, PipelineHealth, PipelineStatus

logger = logging.getLogger(__name__)

_STALE_THRESHOLD_HOURS = 48.0


class PipelineMonitor:
    """Monitors pipeline health and detects stale/failed pipelines."""

    def check_health(self, pipelines: list[Pipeline]) -> list[PipelineHealth]:
        now = datetime.now(timezone.utc)
        results: list[PipelineHealth] = []

        for p in pipelines:
            hours_since: float | None = None
            if p.last_success is not None:
                hours_since = (now - p.last_success).total_seconds() / 3600

            health = PipelineHealth(
                pipeline_id=p.id,
                source=p.chain,
                entity=p.entity,
                schedule=p.schedule,
                last_run=p.last_run,
                last_success=p.last_success,
                status=p.status,
                error=p.last_error,
                hours_since_sync=hours_since,
            )

            # Flag stale pipelines
            if (
                health.hours_since_sync is not None
                and health.hours_since_sync > _STALE_THRESHOLD_HOURS
                and health.status == PipelineStatus.ACTIVE
            ):
                health.status = PipelineStatus.STALE
                health.alert = (
                    f"No successful sync in {health.hours_since_sync:.0f} hours"
                )

            results.append(health)

        return results
