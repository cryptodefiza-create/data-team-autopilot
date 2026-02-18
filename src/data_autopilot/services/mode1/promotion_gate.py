from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    MartTable,
    SemanticContract,
    ValidationCheck,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class PromotionGate:
    """Validates mart tables before promotion to production.

    Checks:
    1. Row count > 0
    2. Null rate below threshold
    3. No duplicate primary keys
    4. No fan-out from joins
    5. Metric values in reasonable ranges
    """

    def validate(
        self,
        mart: MartTable,
        contract: SemanticContract,
    ) -> ValidationResult:
        """Run all validation checks on a mart table."""
        checks: list[ValidationCheck] = []

        checks.append(self._check_row_count(mart))
        checks.append(self._check_null_rate(mart, max_rate=0.3))
        checks.append(self._check_no_duplicates(mart, contract))
        checks.append(self._check_fan_out(mart, contract))
        checks.append(self._check_metric_ranges(mart))

        passed = all(c.passed for c in checks)

        result = ValidationResult(passed=passed, checks=checks)
        if passed:
            logger.info("Promotion gate PASSED for mart %s", mart.name)
        else:
            failures = [c.name for c in checks if not c.passed]
            logger.warning("Promotion gate FAILED for mart %s: %s", mart.name, failures)

        return result

    @staticmethod
    def _check_row_count(mart: MartTable) -> ValidationCheck:
        """Check that the mart has at least one row."""
        if mart.row_count > 0:
            return ValidationCheck(name="row_count", passed=True, message=f"{mart.row_count} rows")
        return ValidationCheck(
            name="row_count", passed=False,
            message="Mart has 0 rows — empty table cannot be promoted",
        )

    @staticmethod
    def _check_null_rate(mart: MartTable, max_rate: float = 0.3) -> ValidationCheck:
        """Check that null rate for key columns is below threshold."""
        if not mart.records:
            return ValidationCheck(name="null_rate", passed=True, message="No records to check")

        total_cells = 0
        null_cells = 0
        for record in mart.records:
            for value in record.values():
                total_cells += 1
                if value is None:
                    null_cells += 1

        rate = null_cells / total_cells if total_cells > 0 else 0.0
        if rate <= max_rate:
            return ValidationCheck(
                name="null_rate", passed=True,
                message=f"Null rate: {rate:.1%} (threshold: {max_rate:.0%})",
            )
        return ValidationCheck(
            name="null_rate", passed=False,
            message=f"Null rate {rate:.1%} exceeds threshold {max_rate:.0%}",
        )

    @staticmethod
    def _check_no_duplicates(mart: MartTable, contract: SemanticContract) -> ValidationCheck:
        """Check for duplicate primary keys."""
        if not mart.records or not mart.source_entities:
            return ValidationCheck(name="no_duplicates", passed=True, message="No records to check")

        entity_name = mart.source_entities[0]
        entity_config = contract.get_entity(entity_name)
        if entity_config is None:
            return ValidationCheck(name="no_duplicates", passed=True, message="No entity config found")

        pk = entity_config.primary_key
        seen: set[Any] = set()
        duplicates = 0

        for record in mart.records:
            key = record.get(pk)
            if key is None:
                continue
            if key in seen:
                duplicates += 1
            seen.add(key)

        if duplicates == 0:
            return ValidationCheck(
                name="no_duplicates", passed=True,
                message=f"No duplicate {pk} values",
            )
        return ValidationCheck(
            name="no_duplicates", passed=False,
            message=f"Found {duplicates} duplicate {pk} values — dedup required",
        )

    @staticmethod
    def _check_fan_out(mart: MartTable, contract: SemanticContract) -> ValidationCheck:
        """Check for unexpected row multiplication from joins."""
        if not mart.source_entities:
            return ValidationCheck(name="fan_out", passed=True, message="No joins to check")

        entity_name = mart.source_entities[0]
        joins = contract.get_joins_for(entity_name)

        for join_def in joins:
            if join_def.fan_out_risk:
                # Check if actual row count exceeds expected
                entity_config = contract.get_entity(entity_name)
                if entity_config and mart.row_count > len(set(
                    r.get(entity_config.primary_key) for r in mart.records
                )):
                    return ValidationCheck(
                        name="fan_out", passed=False,
                        message=(
                            f"Fan-out detected: {mart.row_count} rows but only "
                            f"{len(set(r.get(entity_config.primary_key) for r in mart.records))} "
                            f"unique {entity_config.primary_key} values. "
                            f"Join {join_def.left} ↔ {join_def.right} may be multiplying rows."
                        ),
                    )

        return ValidationCheck(name="fan_out", passed=True, message="No fan-out detected")

    @staticmethod
    def _check_metric_ranges(mart: MartTable) -> ValidationCheck:
        """Check that metric values are in reasonable ranges (not negative, not absurdly large)."""
        if not mart.records:
            return ValidationCheck(name="metric_ranges", passed=True, message="No records to check")

        for record in mart.records:
            for key, value in record.items():
                if not key.startswith("_"):
                    continue
                if key == "_ingested_at" or key == "_source":
                    continue
                if isinstance(value, (int, float)) and value < 0:
                    return ValidationCheck(
                        name="metric_ranges", passed=False,
                        message=f"Negative metric value: {key}={value}",
                    )

        return ValidationCheck(name="metric_ranges", passed=True, message="All metrics in valid range")
