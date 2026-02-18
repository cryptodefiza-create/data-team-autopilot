from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    EntityConfig,
    SemanticContract,
    SnapshotRecord,
    StagingTable,
)
from data_autopilot.services.mode1.persistence import PersistenceManager

logger = logging.getLogger(__name__)


class StagingBuilder:
    """Transforms raw JSONB payloads into typed staging tables.

    In mock mode, flattens in-memory SnapshotRecords into StagingTable objects.
    Production would generate and execute CREATE TABLE AS SELECT SQL.
    """

    def __init__(self, persistence: PersistenceManager) -> None:
        self._persistence = persistence
        self._staging_tables: dict[str, StagingTable] = {}  # f"{org_id}:{entity}" -> table

    def flatten(
        self, org_id: str, entity: str, contract: SemanticContract
    ) -> StagingTable:
        """Flatten raw JSONB snapshots into a typed staging table."""
        entity_config = contract.get_entity(entity)
        if entity_config is None:
            raise ValueError(f"Entity '{entity}' not found in contract")

        backend = self._persistence.get_storage(org_id)
        if backend is None:
            raise ValueError(f"No storage provisioned for org {org_id}")

        snapshots = backend.query_snapshots(entity)
        if not snapshots:
            return StagingTable(name=f"stg_{entity}", entity=entity, row_count=0)

        records = []
        seen_keys: set[str] = set()

        for snap in snapshots:
            record = self._flatten_record(snap, entity_config)
            if record is None:
                continue

            # Apply dedup strategy
            pk_value = str(record.get(entity_config.primary_key, ""))
            if entity_config.dedup_strategy.startswith("latest"):
                # Keep latest by replacing existing
                seen_keys.add(pk_value)
            elif pk_value in seen_keys:
                continue
            else:
                seen_keys.add(pk_value)

            # Apply exclusions
            if self._should_exclude(record, entity_config.exclusions):
                continue

            records.append(record)

        columns = list(records[0].keys()) if records else []

        staging = StagingTable(
            name=f"stg_{entity}",
            entity=entity,
            row_count=len(records),
            columns=columns,
            records=records,
        )

        self._staging_tables[f"{org_id}:{entity}"] = staging
        logger.info(
            "Flattened %d raw records → %d staging rows for %s:%s",
            len(snapshots), len(records), org_id, entity,
        )
        return staging

    def get_staging_table(self, org_id: str, entity: str) -> StagingTable | None:
        return self._staging_tables.get(f"{org_id}:{entity}")

    def _flatten_record(
        self, snapshot: SnapshotRecord, config: EntityConfig
    ) -> dict[str, Any] | None:
        """Extract typed fields from a JSONB payload."""
        payload = snapshot.payload
        if not payload:
            return None

        record: dict[str, Any] = {}

        pk = payload.get(config.primary_key)
        if pk is not None:
            record[config.primary_key] = pk

        for key, value in payload.items():
            if key not in record:
                record[key] = value

        record["_ingested_at"] = snapshot.ingested_at.isoformat()
        record["_source"] = snapshot.source

        return record

    @staticmethod
    def _should_exclude(record: dict[str, Any], exclusions: list[str]) -> bool:
        """Check if a record matches any exclusion rules."""
        for exclusion in exclusions:
            excl_lower = exclusion.lower()
            if "test" in excl_lower:
                if record.get("test", False):
                    return True
                tags = str(record.get("tags", "")).lower()
                if "test" in tags:
                    return True
            if "cancelled" in excl_lower or "canceled" in excl_lower:
                status = str(record.get("status", "")).lower()
                if status in ("cancelled", "canceled"):
                    return True
        return False
