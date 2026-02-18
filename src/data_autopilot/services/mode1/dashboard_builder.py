from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from data_autopilot.services.mode1.persistence import PersistenceManager

logger = logging.getLogger(__name__)


class DashboardBuilder:
    """Creates basic dashboards from stored snapshot data."""

    def __init__(self, persistence: PersistenceManager) -> None:
        self._persistence = persistence
        self._dashboards: dict[str, dict[str, Any]] = {}

    def create_from_snapshots(
        self,
        org_id: str,
        entity: str,
        title: str = "",
    ) -> dict[str, Any]:
        backend = self._persistence.get_storage(org_id)
        if backend is None:
            return {
                "status": "error",
                "message": "No stored data available for dashboard.",
            }

        snapshots = backend.query_snapshots(entity=entity)
        if not snapshots:
            return {
                "status": "error",
                "message": f"No snapshot data for '{entity}' to build dashboard.",
            }

        # Group snapshots by ingestion date for trend
        daily_counts: dict[str, int] = {}
        for snap in snapshots:
            day = snap.ingested_at.strftime("%Y-%m-%d")
            daily_counts[day] = daily_counts.get(day, 0) + 1

        chart_data = [
            {"date": day, "record_count": count}
            for day, count in sorted(daily_counts.items())
        ]

        dash_id = f"dash_{uuid4().hex[:10]}"
        dashboard = {
            "id": dash_id,
            "title": title or f"{entity} Tracking Dashboard",
            "org_id": org_id,
            "entity": entity,
            "charts": [
                {
                    "type": "line",
                    "title": f"{entity} records over time",
                    "data": chart_data,
                    "x_axis": "date",
                    "y_axis": "record_count",
                }
            ],
            "total_snapshots": len(snapshots),
            "date_range": {
                "start": chart_data[0]["date"] if chart_data else None,
                "end": chart_data[-1]["date"] if chart_data else None,
            },
        }

        self._dashboards[dash_id] = dashboard
        logger.info("Created dashboard %s for org %s entity %s",
                     dash_id, org_id, entity)
        return {"status": "success", "dashboard": dashboard}

    def get_dashboard(self, dashboard_id: str) -> dict[str, Any] | None:
        return self._dashboards.get(dashboard_id)
