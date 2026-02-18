from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from data_autopilot.services.mode1.models import SemanticContract
from data_autopilot.services.mode1.semantic_contract import SemanticContractManager

logger = logging.getLogger(__name__)


class ContractVersionManager:
    """Manages semantic contract versioning, rollback, and alias switching."""

    def __init__(self, contract_manager: SemanticContractManager) -> None:
        self._manager = contract_manager
        # org_id -> {version -> contract}
        self._versions: dict[str, dict[int, SemanticContract]] = {}
        # org_id -> active version number
        self._active_version: dict[str, int] = {}

    def update(self, org_id: str, changes: dict[str, Any]) -> int:
        """Apply changes to create a new contract version. Returns new version number."""
        current = self._manager.get(org_id)
        if current is None:
            raise ValueError(f"No contract found for org {org_id}")

        # Save current version to history
        if org_id not in self._versions:
            self._versions[org_id] = {}
        self._versions[org_id][current.version] = current.model_copy(deep=True)

        # Apply changes to create new version
        updated = self._manager.apply_changes(org_id, changes)
        new_version = current.version + 1
        updated.version = new_version
        updated.effective_date = datetime.now(timezone.utc)

        # Store new version
        self._versions[org_id][new_version] = updated.model_copy(deep=True)
        self._manager.store(org_id, updated)
        self._active_version[org_id] = new_version

        logger.info("Updated contract for org %s: v%d → v%d", org_id, current.version, new_version)
        return new_version

    def rollback(self, org_id: str) -> int:
        """Rollback to the previous contract version. Returns the restored version number."""
        current = self._manager.get(org_id)
        if current is None:
            raise ValueError(f"No contract found for org {org_id}")

        previous_version = current.version - 1
        if previous_version < 1:
            raise ValueError("Cannot rollback: already at version 1")

        versions = self._versions.get(org_id, {})
        previous = versions.get(previous_version)
        if previous is None:
            raise ValueError(f"Version {previous_version} not found in history")

        self._manager.store(org_id, previous.model_copy(deep=True))
        self._active_version[org_id] = previous_version

        logger.info("Rolled back contract for org %s to v%d", org_id, previous_version)
        return previous_version

    def get_version(self, org_id: str, version: int) -> SemanticContract | None:
        """Get a specific version of a contract."""
        return self._versions.get(org_id, {}).get(version)

    def get_active_version(self, org_id: str) -> int:
        return self._active_version.get(org_id, 1)

    def list_versions(self, org_id: str) -> list[int]:
        return sorted(self._versions.get(org_id, {}).keys())

    def compare_versions(
        self, org_id: str, v1: int, v2: int
    ) -> dict[str, Any]:
        """Compare two contract versions and return differences."""
        versions = self._versions.get(org_id, {})
        contract_v1 = versions.get(v1)
        contract_v2 = versions.get(v2)

        if contract_v1 is None or contract_v2 is None:
            return {"error": f"Version {v1 if contract_v1 is None else v2} not found"}

        diffs: list[dict[str, Any]] = []

        # Compare metrics
        v1_metrics = {m.name: m for m in contract_v1.metrics}
        v2_metrics = {m.name: m for m in contract_v2.metrics}

        for name in set(list(v1_metrics.keys()) + list(v2_metrics.keys())):
            m1 = v1_metrics.get(name)
            m2 = v2_metrics.get(name)
            if m1 and m2 and m1.definition != m2.definition:
                diffs.append({
                    "type": "metric_changed",
                    "name": name,
                    f"v{v1}": m1.definition,
                    f"v{v2}": m2.definition,
                })
            elif m1 and not m2:
                diffs.append({"type": "metric_removed", "name": name})
            elif m2 and not m1:
                diffs.append({"type": "metric_added", "name": name})

        # Compare defaults
        if contract_v1.defaults != contract_v2.defaults:
            diffs.append({
                "type": "defaults_changed",
                f"v{v1}": contract_v1.defaults.model_dump(),
                f"v{v2}": contract_v2.defaults.model_dump(),
            })

        return {
            "org_id": org_id,
            "v1": v1,
            "v2": v2,
            "differences": diffs,
            "total_changes": len(diffs),
        }
