from __future__ import annotations

import logging
from datetime import datetime, timezone
from data_autopilot.services.mode1.models import Pipeline, PipelineStatus

logger = logging.getLogger(__name__)

SCHEDULE_MAP: dict[str, str] = {
    "hourly": "0 * * * *",
    "daily": "0 6 * * *",       # 6 AM UTC
    "weekly": "0 6 * * 1",      # Monday 6 AM UTC
}

# Hours between expected runs for staleness detection
SCHEDULE_INTERVAL_HOURS: dict[str, float] = {
    "hourly": 1.0,
    "daily": 24.0,
    "weekly": 168.0,
}


class PipelineScheduler:
    """Manages pipeline scheduling. In mock mode, runs are triggered manually."""

    def __init__(self) -> None:
        self._scheduled: dict[str, str] = {}  # pipeline_id → cron expression

    def schedule(self, pipeline: Pipeline) -> str:
        cron = SCHEDULE_MAP.get(pipeline.schedule, SCHEDULE_MAP["daily"])
        self._scheduled[pipeline.id] = cron
        logger.info("Scheduled pipeline %s with cron: %s", pipeline.id, cron)
        return cron

    def unschedule(self, pipeline_id: str) -> None:
        self._scheduled.pop(pipeline_id, None)

    def get_cron(self, pipeline_id: str) -> str | None:
        return self._scheduled.get(pipeline_id)

    def get_due_pipelines(
        self, pipelines: list[Pipeline], now: datetime | None = None
    ) -> list[Pipeline]:
        """Return pipelines that are due for execution based on schedule."""
        if now is None:
            now = datetime.now(timezone.utc)

        due: list[Pipeline] = []
        for p in pipelines:
            if p.status != PipelineStatus.ACTIVE:
                continue
            if p.id not in self._scheduled:
                continue

            interval = SCHEDULE_INTERVAL_HOURS.get(p.schedule, 24.0)
            if p.last_run is None:
                due.append(p)
            else:
                hours_since = (now - p.last_run).total_seconds() / 3600
                if hours_since >= interval:
                    due.append(p)

        return due

    @property
    def scheduled_count(self) -> int:
        return len(self._scheduled)
