from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

from data_autopilot.services.mode1.models import (
    DataRequest,
    Pipeline,
    PipelineStatus,
)
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.live_fetcher import LiveFetcher

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3


class SnapshotPipeline:
    """Creates and runs recurring data collection pipelines."""

    def __init__(
        self,
        persistence: PersistenceManager,
        fetcher: LiveFetcher,
    ) -> None:
        self._persistence = persistence
        self._fetcher = fetcher
        self._pipelines: dict[str, Pipeline] = {}

    def create(
        self,
        org_id: str,
        request: DataRequest,
        schedule: str = "daily",
    ) -> Pipeline:
        pipeline = Pipeline(
            id=f"pipe_{uuid4().hex[:10]}",
            org_id=org_id,
            entity=request.entity.value,
            chain=request.chain.value,
            token=request.token,
            address=request.address,
            query_params={
                "token": request.token,
                "address": request.address,
                "mint": request.token,
            },
            schedule=schedule,
            status=PipelineStatus.ACTIVE,
        )
        self._pipelines[pipeline.id] = pipeline
        logger.info("Created pipeline %s for org %s: %s %s",
                     pipeline.id, org_id, request.entity.value, schedule)

        # Run first snapshot immediately
        self.run(pipeline)
        return pipeline

    def run(self, pipeline: Pipeline) -> bool:
        """Execute a single pipeline run. Returns True on success."""
        retries = 0
        last_error = ""

        while retries < _MAX_RETRIES:
            try:
                # Fetch current data from provider
                request = DataRequest(
                    raw_message=f"Scheduled fetch: {pipeline.entity}",
                    entity=pipeline.entity,
                    chain=pipeline.chain,
                    token=pipeline.token,
                    address=pipeline.address,
                )
                result = self._fetcher.execute(request, session_id=f"pipe_{pipeline.id}")

                if result.get("response_type") == "error":
                    raise RuntimeError(result.get("summary", "Provider error"))

                records = result.get("data", {}).get("records", [])
                if not records:
                    raise RuntimeError("No records returned from provider")

                # Store in raw/ as JSONB
                self._persistence.store_snapshot(
                    org_id=pipeline.org_id,
                    source=pipeline.entity,
                    entity=pipeline.entity,
                    query_params=pipeline.query_params,
                    records=records,
                )

                # Update pipeline status
                now = datetime.now(timezone.utc)
                pipeline.last_run = now
                pipeline.last_success = now
                pipeline.last_error = None
                pipeline.run_count += 1
                pipeline.status = PipelineStatus.ACTIVE
                return True

            except Exception as exc:
                retries += 1
                last_error = str(exc)
                logger.warning(
                    "Pipeline %s attempt %d failed: %s",
                    pipeline.id, retries, exc,
                )

        # All retries exhausted
        pipeline.last_run = datetime.now(timezone.utc)
        pipeline.last_error = last_error
        pipeline.status = PipelineStatus.FAILED
        logger.error("Pipeline %s failed after %d retries: %s",
                      pipeline.id, _MAX_RETRIES, last_error)
        return False

    def get_pipeline(self, pipeline_id: str) -> Pipeline | None:
        return self._pipelines.get(pipeline_id)

    def get_active_pipelines(self, org_id: str) -> list[Pipeline]:
        return [
            p for p in self._pipelines.values()
            if p.org_id == org_id and p.status == PipelineStatus.ACTIVE
        ]

    def get_all_pipelines(self, org_id: str) -> list[Pipeline]:
        return [p for p in self._pipelines.values() if p.org_id == org_id]

    def pause_pipeline(self, pipeline_id: str) -> None:
        p = self._pipelines.get(pipeline_id)
        if p:
            p.status = PipelineStatus.PAUSED
