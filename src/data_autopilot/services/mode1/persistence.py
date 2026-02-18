from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

from data_autopilot.services.mode1.models import SnapshotRecord, StorageConfig

logger = logging.getLogger(__name__)

_AGENT_SCHEMAS = ["raw", "staging", "marts", "analytics"]

# Tier limits for persistence
TIER_PERSISTENCE = {
    "free": False,
    "starter": False,
    "pro": True,
}


class TierLimitError(ValueError):
    pass


class RBACViolation(PermissionError):
    pass


class StorageBackend:
    """Abstract storage backend. Mock mode uses in-memory dicts."""

    def insert_snapshot(self, record: SnapshotRecord) -> int:
        raise NotImplementedError

    def query_snapshots(
        self,
        entity: str,
        as_of: datetime | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> list[SnapshotRecord]:
        raise NotImplementedError

    def count_snapshots(self, entity: str = "") -> int:
        raise NotImplementedError


class MockStorageBackend(StorageBackend):
    """In-memory storage for development and testing."""

    def __init__(self) -> None:
        self._records: list[SnapshotRecord] = []
        self._next_id = 1
        self._schemas_created: set[str] = set()
        self._agent_role_created = False

    def insert_snapshot(self, record: SnapshotRecord) -> int:
        # Check schema RBAC — only agent-managed schemas allowed
        self._check_rbac("raw")

        # Dedup by payload_hash
        for existing in self._records:
            if (
                existing.payload_hash == record.payload_hash
                and existing.entity == record.entity
            ):
                return existing.id

        record.id = self._next_id
        self._next_id += 1
        self._records.append(record)
        return record.id

    def query_snapshots(
        self,
        entity: str,
        as_of: datetime | None = None,
        query_params: dict[str, Any] | None = None,
    ) -> list[SnapshotRecord]:
        results = [r for r in self._records if r.entity == entity]

        if as_of is not None:
            results = [r for r in results if r.ingested_at <= as_of]

        if query_params:
            filtered = []
            for r in results:
                match = all(
                    r.query_params.get(k) == v for k, v in query_params.items()
                )
                if match:
                    filtered.append(r)
            results = filtered

        # Return most recent per record_id (like DISTINCT ON)
        seen: dict[str, SnapshotRecord] = {}
        for r in sorted(results, key=lambda x: x.ingested_at):
            seen[r.record_id] = r
        return list(seen.values())

    def count_snapshots(self, entity: str = "") -> int:
        if entity:
            return sum(1 for r in self._records if r.entity == entity)
        return len(self._records)

    def create_schemas(self) -> None:
        self._schemas_created = set(_AGENT_SCHEMAS)

    def create_agent_role(self) -> None:
        self._agent_role_created = True

    @property
    def schemas_exist(self) -> bool:
        return self._schemas_created == set(_AGENT_SCHEMAS)

    @property
    def has_agent_role(self) -> bool:
        return self._agent_role_created

    def _check_rbac(self, schema: str) -> None:
        if schema not in _AGENT_SCHEMAS:
            raise RBACViolation(
                f"Agent role cannot write to schema '{schema}'. "
                f"Allowed: {_AGENT_SCHEMAS}"
            )

    def write_to_schema(self, schema: str, data: Any) -> None:
        """Explicit schema write for RBAC testing."""
        self._check_rbac(schema)


class PersistenceManager:
    """Manages storage provisioning and access per org."""

    def __init__(self, mock_mode: bool = True) -> None:
        self._mock_mode = mock_mode
        self._storages: dict[str, StorageBackend] = {}
        self._configs: dict[str, StorageConfig] = {}

    def ensure_storage(self, org_id: str, tier: str = "pro") -> StorageConfig:
        if org_id in self._configs:
            return self._configs[org_id]

        if not TIER_PERSISTENCE.get(tier, False):
            raise TierLimitError(
                "Persistence requires Pro tier or above. "
                "Upgrade to track data over time."
            )

        if self._mock_mode:
            backend = MockStorageBackend()
            backend.create_schemas()
            backend.create_agent_role()
            config = StorageConfig(
                type="mock",
                project_id=f"mock-{uuid4().hex[:8]}",
                connection_uri=f"mock://autopilot-{org_id}",
            )
        else:
            raise NotImplementedError("Neon provisioning not yet implemented")

        self._storages[org_id] = backend
        self._configs[org_id] = config
        logger.info("Provisioned storage for org %s: %s", org_id, config.type)
        return config

    def get_storage(self, org_id: str) -> StorageBackend | None:
        return self._storages.get(org_id)

    def has_storage(self, org_id: str) -> bool:
        return org_id in self._storages

    def store_snapshot(
        self,
        org_id: str,
        source: str,
        entity: str,
        query_params: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> int:
        """Store fetched records as JSONB snapshots. Returns count of new records."""
        backend = self._storages.get(org_id)
        if backend is None:
            raise ValueError(f"No storage provisioned for org {org_id}")

        stored = 0
        for record in records:
            record_id = _extract_primary_key(record, entity)
            payload_hash = hashlib.sha256(
                json.dumps(record, sort_keys=True).encode()
            ).hexdigest()

            snap = SnapshotRecord(
                source=source,
                entity=entity,
                query_params=query_params,
                record_id=record_id,
                payload_hash=payload_hash,
                payload=record,
            )
            backend.insert_snapshot(snap)
            stored += 1

        return stored


def _extract_primary_key(record: dict[str, Any], entity: str) -> str:
    """Extract a primary key from the record based on entity type."""
    for key in ("wallet", "address", "owner", "pairAddress", "id", "contract"):
        if key in record:
            return str(record[key])
    return hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()[:16]
