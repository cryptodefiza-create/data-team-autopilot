from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    MartTable,
    MetricDefinition,
    SemanticContract,
    StagingTable,
)

logger = logging.getLogger(__name__)


class MartGenerator:
    """Builds business-ready mart tables from staging tables + semantic contract.

    In mock mode, performs in-memory joins and aggregations.
    Production would generate and execute SQL.
    """

    def __init__(self) -> None:
        self._marts: dict[str, MartTable] = {}  # f"{org_id}:{mart_name}" -> table

    def generate(
        self,
        org_id: str,
        contract: SemanticContract,
        staging_tables: dict[str, StagingTable],
    ) -> list[MartTable]:
        """Generate mart tables from staging tables based on contract."""
        marts: list[MartTable] = []

        for metric in contract.metrics:
            mart = self._build_metric_mart(org_id, metric, contract, staging_tables)
            if mart:
                self._marts[f"{org_id}:{mart.name}"] = mart
                marts.append(mart)

        logger.info("Generated %d marts for org %s", len(marts), org_id)
        return marts

    def get_mart(self, org_id: str, mart_name: str) -> MartTable | None:
        return self._marts.get(f"{org_id}:{mart_name}")

    def _build_metric_mart(
        self,
        org_id: str,
        metric: MetricDefinition,
        contract: SemanticContract,
        staging_tables: dict[str, StagingTable],
    ) -> MartTable | None:
        """Build a mart table for a single metric."""
        # Find the primary entity for this metric
        primary_entity = self._find_metric_entity(metric, contract)
        if primary_entity is None:
            logger.warning("No entity found for metric %s", metric.name)
            return None

        staging = staging_tables.get(primary_entity)
        if staging is None:
            logger.warning("No staging table for entity %s", primary_entity)
            return None

        # Apply joins if defined
        records = list(staging.records)
        for join_def in contract.get_joins_for(primary_entity):
            other_entity = join_def.right if join_def.left == primary_entity else join_def.left
            other_staging = staging_tables.get(other_entity)
            if other_staging:
                records = self._apply_join(records, other_staging.records, join_def)

        # Compute metric values
        for record in records:
            record[f"_{metric.name}"] = self._compute_metric(record, metric)

        mart = MartTable(
            name=f"mart_{metric.name}",
            source_entities=[primary_entity],
            row_count=len(records),
            columns=list(records[0].keys()) if records else [],
            records=records,
            version=contract.version,
        )

        return mart

    @staticmethod
    def _find_metric_entity(
        metric: MetricDefinition, contract: SemanticContract
    ) -> str | None:
        """Determine which entity a metric belongs to."""
        definition = metric.definition.lower()
        for entity in contract.entities:
            # Check if any entity column appears in the metric definition
            for col in entity.columns:
                if col.name.lower() in definition:
                    return entity.name
        # Default to first entity
        return contract.entities[0].name if contract.entities else None

    @staticmethod
    def _apply_join(
        left_records: list[dict[str, Any]],
        right_records: list[dict[str, Any]],
        join_def: Any,
    ) -> list[dict[str, Any]]:
        """Apply an in-memory join between two record sets."""
        # Parse join condition: "order.customer_id = customer.id"
        on_parts = join_def.on.replace(" ", "").split("=")
        if len(on_parts) != 2:
            return left_records

        left_key = on_parts[0].split(".")[-1]
        right_key = on_parts[1].split(".")[-1]

        # Build index on right side
        right_index: dict[Any, list[dict[str, Any]]] = {}
        for r in right_records:
            key = r.get(right_key)
            if key is not None:
                right_index.setdefault(key, []).append(r)

        # Perform join
        joined = []
        for left in left_records:
            key = left.get(left_key)
            matches = right_index.get(key, [])
            if matches:
                for right in matches:
                    merged = {**left}
                    for k, v in right.items():
                        if k not in merged:
                            merged[k] = v
                    joined.append(merged)
            elif join_def.type == "left":
                joined.append(left)

        return joined if joined else left_records

    @staticmethod
    def _compute_metric(record: dict[str, Any], metric: MetricDefinition) -> float:
        """Compute a metric value for a single record."""
        # Parse simple SUM expressions
        definition = metric.definition.lower()

        total = 0.0
        if "order_amount" in definition or "amount" in definition:
            total += float(record.get("amount", record.get("order_amount", 0)) or 0)

        if "refund_amount" in definition and "-" in definition:
            total -= float(record.get("refund_amount", 0) or 0)

        if "tax_amount" in definition and "-" in definition:
            total -= float(record.get("tax_amount", record.get("tax", 0)) or 0)

        return total
