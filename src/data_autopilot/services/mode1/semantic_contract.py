from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    JoinDefinition,
    MetricDefinition,
    SemanticContract,
)

logger = logging.getLogger(__name__)


class SemanticContractManager:
    """Manages full semantic contracts per org.

    Extends the thin contract (Phase 4) with entities, metrics, joins,
    and defaults for full enterprise data correctness.
    """

    def __init__(self) -> None:
        self._contracts: dict[str, SemanticContract] = {}  # org_id -> current contract

    def create(
        self,
        org_id: str,
        entities: list[EntityConfig] | None = None,
        metrics: list[MetricDefinition] | None = None,
        joins: list[JoinDefinition] | None = None,
        defaults: ContractDefaults | None = None,
    ) -> SemanticContract:
        contract = SemanticContract(
            org_id=org_id,
            version=1,
            entities=entities or [],
            metrics=metrics or [],
            joins=joins or [],
            defaults=defaults or ContractDefaults(),
        )
        self._contracts[org_id] = contract
        logger.info("Created semantic contract v%d for org %s", contract.version, org_id)
        return contract

    def get(self, org_id: str) -> SemanticContract | None:
        return self._contracts.get(org_id)

    def has_contract(self, org_id: str) -> bool:
        return org_id in self._contracts

    def store(self, org_id: str, contract: SemanticContract) -> None:
        self._contracts[org_id] = contract

    def apply_changes(self, org_id: str, changes: dict[str, Any]) -> SemanticContract:
        """Apply changes to an existing contract, returning the updated version."""
        current = self._contracts.get(org_id)
        if current is None:
            raise ValueError(f"No contract found for org {org_id}")

        updated = current.model_copy(deep=True)

        if "defaults" in changes:
            for k, v in changes["defaults"].items():
                setattr(updated.defaults, k, v)

        if "metrics" in changes:
            for metric_change in changes["metrics"]:
                name = metric_change.get("name", "")
                existing = updated.get_metric(name)
                if existing:
                    for k, v in metric_change.items():
                        setattr(existing, k, v)
                else:
                    updated.metrics.append(MetricDefinition(**metric_change))

        if "entities" in changes:
            for entity_change in changes["entities"]:
                name = entity_change.get("name", "")
                existing = updated.get_entity(name)
                if existing:
                    for k, v in entity_change.items():
                        setattr(existing, k, v)
                else:
                    updated.entities.append(EntityConfig(**entity_change))

        if "joins" in changes:
            updated.joins = [JoinDefinition(**j) for j in changes["joins"]]

        return updated
