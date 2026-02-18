from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from data_autopilot.services.mode1.persistence import PersistenceManager

logger = logging.getLogger(__name__)


class HistoricalQuery:
    """Query stored snapshots for historical data."""

    def __init__(self, persistence: PersistenceManager) -> None:
        self._persistence = persistence

    def query_snapshot(
        self,
        org_id: str,
        entity: str,
        as_of: datetime | None = None,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        backend = self._persistence.get_storage(org_id)
        if backend is None:
            return {
                "response_type": "error",
                "summary": (
                    "No tracking data available. Use 'track this' to start "
                    "collecting data, then query historical snapshots later."
                ),
                "data": {},
                "warnings": ["no_storage"],
            }

        results = backend.query_snapshots(
            entity=entity,
            as_of=as_of,
            query_params=filters,
        )

        if not results:
            if as_of is not None:
                return {
                    "response_type": "info",
                    "summary": (
                        f"No snapshot data found for '{entity}' as of "
                        f"{as_of.strftime('%b %d, %Y')}. "
                        "Tracking may have started after this date. "
                        "Historical data is only available from when tracking began."
                    ),
                    "data": {"entity": entity, "as_of": as_of.isoformat()},
                    "warnings": ["no_historical_data"],
                }
            return {
                "response_type": "info",
                "summary": f"No snapshot data found for '{entity}'.",
                "data": {},
                "warnings": ["empty_snapshots"],
            }

        records = [r.payload for r in results]
        return {
            "response_type": "blockchain_result",
            "summary": f"Found {len(records)} historical records for '{entity}'.",
            "data": {
                "records": records,
                "total_available": len(records),
                "truncated": False,
                "source": "stored_snapshots",
            },
            "warnings": [],
        }

    def get_trend(
        self,
        org_id: str,
        entity: str,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get all snapshots over time for trend analysis."""
        backend = self._persistence.get_storage(org_id)
        if backend is None:
            return {
                "response_type": "error",
                "summary": "No tracking data available.",
                "data": {},
                "warnings": ["no_storage"],
            }

        results = backend.query_snapshots(
            entity=entity, query_params=filters
        )

        records = [
            {
                "ingested_at": r.ingested_at.isoformat(),
                "record_id": r.record_id,
                **r.payload,
            }
            for r in results
        ]

        return {
            "response_type": "blockchain_result",
            "summary": f"Found {len(records)} trend records for '{entity}'.",
            "data": {
                "records": records,
                "total_available": len(records),
                "truncated": False,
                "source": "stored_snapshots",
            },
            "warnings": [],
        }
