from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from data_autopilot.services.mode1.models import SyncStatus

logger = logging.getLogger(__name__)


class AirbyteClient:
    """Client for Airbyte Cloud API. Mock mode simulates connection lifecycle."""

    def __init__(self, mock_mode: bool = True, api_key: str = "") -> None:
        self._mock_mode = mock_mode
        self._api_key = api_key
        self._connections: dict[str, dict[str, Any]] = {}
        self._syncs: dict[str, SyncStatus] = {}

    def create_connection(
        self,
        source_config: dict[str, Any],
        destination_config: dict[str, Any],
        schedule: str = "0 6 * * *",
    ) -> str:
        """Create an Airbyte connection (source → destination with schedule)."""
        connection_id = f"conn_{uuid4().hex[:10]}"

        self._connections[connection_id] = {
            "source": source_config,
            "destination": destination_config,
            "schedule": schedule,
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Created Airbyte connection %s: %s → %s",
            connection_id,
            source_config.get("source_type", "unknown"),
            destination_config.get("destination_type", "unknown"),
        )
        return connection_id

    def trigger_sync(self, connection_id: str) -> SyncStatus:
        """Trigger a manual sync for a connection."""
        if connection_id not in self._connections:
            return SyncStatus(
                connection_id=connection_id,
                status="failed",
                error=f"Connection {connection_id} not found",
            )

        sync = SyncStatus(
            connection_id=connection_id,
            status="running",
            started_at=datetime.now(timezone.utc),
        )

        if self._mock_mode:
            # In mock mode, sync completes immediately
            sync.status = "completed"
            sync.completed_at = datetime.now(timezone.utc)
            sync.rows_synced = 1000  # Simulated

        self._syncs[connection_id] = sync
        return sync

    def get_sync_status(self, connection_id: str) -> SyncStatus:
        """Get the current sync status for a connection."""
        return self._syncs.get(
            connection_id,
            SyncStatus(connection_id=connection_id, status="pending"),
        )

    def get_connection(self, connection_id: str) -> dict[str, Any] | None:
        return self._connections.get(connection_id)

    def delete_connection(self, connection_id: str) -> bool:
        if connection_id in self._connections:
            del self._connections[connection_id]
            self._syncs.pop(connection_id, None)
            return True
        return False

    @property
    def connection_count(self) -> int:
        return len(self._connections)
