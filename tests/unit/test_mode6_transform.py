"""Phase 6 tests: Staging builder, mart generator, promotion gate, DAG ordering."""

from datetime import datetime, timezone

from data_autopilot.services.mode1.mart_generator import MartGenerator
from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    JoinDefinition,
    MartTable,
    MetricDefinition,
    SemanticContract,
    SnapshotRecord,
)
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.promotion_gate import PromotionGate
from data_autopilot.services.mode1.staging_builder import StagingBuilder
from data_autopilot.services.mode1.transform_dag import TransformDAG


def _make_contract_and_storage():
    """Create contract + storage with 1000 raw JSONB records."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_t", tier="pro")
    backend = persistence.get_storage("org_t")

    for i in range(1000):
        snap = SnapshotRecord(
            source="shopify",
            entity="order",
            query_params={"store": "test"},
            record_id=f"order_{i}",
            payload_hash=f"hash_{i}",
            payload={
                "order_id": f"ord_{i}",
                "customer_id": f"cust_{i % 100}",
                "amount": 50.0 + (i % 50),
                "refund_amount": 5.0 if i % 10 == 0 else 0.0,
                "status": "cancelled" if i % 20 == 0 else "completed",
                "tags": "test" if i % 50 == 0 else "",
            },
            ingested_at=datetime(2025, 3, (i % 28) + 1, 12, 0, tzinfo=timezone.utc),
        )
        backend.insert_snapshot(snap)

    contract = SemanticContract(
        org_id="org_t",
        entities=[
            EntityConfig(
                name="order",
                grain="one row per order",
                source_table="staging.stg_orders",
                primary_key="order_id",
                exclusions=["test orders", "cancelled orders"],
            ),
        ],
        metrics=[
            MetricDefinition(
                name="revenue",
                definition="SUM(order_amount) - SUM(refund_amount)",
                includes_refunds=True,
            ),
        ],
        defaults=ContractDefaults(timezone="UTC"),
    )

    return persistence, contract


def test_jsonb_to_staging() -> None:
    """6.5: 1,000 raw JSONB records → typed staging table with correct columns."""
    persistence, contract = _make_contract_and_storage()
    builder = StagingBuilder(persistence)

    staging = builder.flatten("org_t", "order", contract)

    assert staging.name == "stg_order"
    assert staging.row_count > 0
    # Should exclude test orders and cancelled orders
    assert staging.row_count < 1000
    assert "order_id" in staging.columns
    assert "amount" in staging.columns
    assert "_ingested_at" in staging.columns

    # Verify no test/cancelled orders made it through
    for record in staging.records:
        assert record.get("tags", "") != "test" or record.get("status") != "cancelled"


def test_mart_generation() -> None:
    """6.6: Staging tables + contract → mart with correct aggregation."""
    persistence, contract = _make_contract_and_storage()
    builder = StagingBuilder(persistence)
    staging = builder.flatten("org_t", "order", contract)

    generator = MartGenerator()
    marts = generator.generate(
        "org_t",
        contract,
        {"order": staging},
    )

    assert len(marts) == 1
    mart = marts[0]
    assert mart.name == "mart_revenue"
    assert mart.row_count > 0
    assert mart.version == contract.version

    # Revenue metric should be computed
    for record in mart.records:
        assert "_revenue" in record


def test_promotion_gate_pass() -> None:
    """6.7: Valid mart with no issues → passes all checks."""
    contract = SemanticContract(
        org_id="org_pg",
        entities=[EntityConfig(name="order", primary_key="order_id")],
        metrics=[MetricDefinition(name="revenue", definition="SUM(amount)")],
    )

    mart = MartTable(
        name="mart_revenue",
        source_entities=["order"],
        row_count=3,
        records=[
            {"order_id": "1", "amount": 100, "_revenue": 100},
            {"order_id": "2", "amount": 200, "_revenue": 200},
            {"order_id": "3", "amount": 150, "_revenue": 150},
        ],
    )

    gate = PromotionGate()
    result = gate.validate(mart, contract)

    assert result.passed
    assert len(result.failures) == 0


def test_promotion_gate_fan_out() -> None:
    """6.8: Join that doubles rows → fails fan-out check."""
    contract = SemanticContract(
        org_id="org_fo",
        entities=[EntityConfig(name="order", primary_key="order_id")],
        joins=[JoinDefinition(
            left="order", right="line_item",
            on="order.order_id = line_item.order_id",
            expected_cardinality="1:N",
            fan_out_risk=True,
        )],
    )

    # Mart has duplicate order_ids (fan-out from join)
    mart = MartTable(
        name="mart_revenue",
        source_entities=["order"],
        row_count=6,
        records=[
            {"order_id": "1", "amount": 100, "_revenue": 100},
            {"order_id": "1", "amount": 50, "_revenue": 50},
            {"order_id": "2", "amount": 200, "_revenue": 200},
            {"order_id": "2", "amount": 75, "_revenue": 75},
            {"order_id": "3", "amount": 150, "_revenue": 150},
            {"order_id": "3", "amount": 80, "_revenue": 80},
        ],
    )

    gate = PromotionGate()
    result = gate.validate(mart, contract)

    assert not result.passed
    fan_out_check = next((c for c in result.checks if c.name == "fan_out"), None)
    assert fan_out_check is not None
    assert not fan_out_check.passed
    assert "fan-out" in fan_out_check.message.lower()


def test_promotion_gate_duplicates() -> None:
    """6.9: Mart with duplicate keys → fails dedup check."""
    contract = SemanticContract(
        org_id="org_dup",
        entities=[EntityConfig(name="order", primary_key="order_id")],
    )

    mart = MartTable(
        name="mart_revenue",
        source_entities=["order"],
        row_count=4,
        records=[
            {"order_id": "1", "amount": 100},
            {"order_id": "1", "amount": 100},  # Duplicate!
            {"order_id": "2", "amount": 200},
            {"order_id": "3", "amount": 150},
        ],
    )

    gate = PromotionGate()
    result = gate.validate(mart, contract)

    assert not result.passed
    dedup_check = next((c for c in result.checks if c.name == "no_duplicates"), None)
    assert dedup_check is not None
    assert not dedup_check.passed
    assert "duplicate" in dedup_check.message.lower()


def test_dag_ordering() -> None:
    """6.10: raw → staging → marts → executes in correct order."""
    dag = TransformDAG()

    dag.add_node("raw_orders", "raw")
    dag.add_node("stg_orders", "staging", depends_on=["raw_orders"])
    dag.add_node("mart_revenue", "marts", depends_on=["stg_orders"])

    order = dag.get_execution_order()
    assert order.index("raw_orders") < order.index("stg_orders")
    assert order.index("stg_orders") < order.index("mart_revenue")

    # Execute all
    execution_log: list[str] = []
    runners = {
        "raw_orders": lambda: execution_log.append("raw_orders"),
        "stg_orders": lambda: execution_log.append("stg_orders"),
        "mart_revenue": lambda: execution_log.append("mart_revenue"),
    }

    results = dag.execute(runners)
    assert all(r["status"] == "completed" for r in results.values())
    assert execution_log == ["raw_orders", "stg_orders", "mart_revenue"]


def test_dag_blocking() -> None:
    """6.11: Staging fails → dependent marts not executed, blocked."""
    dag = TransformDAG()

    dag.add_node("raw_orders", "raw")
    dag.add_node("stg_orders", "staging", depends_on=["raw_orders"])
    dag.add_node("mart_revenue", "marts", depends_on=["stg_orders"])

    def failing_staging():
        raise RuntimeError("Staging transformation failed: invalid data")

    runners = {
        "raw_orders": lambda: "ok",
        "stg_orders": failing_staging,
        "mart_revenue": lambda: "should not run",
    }

    results = dag.execute(runners)

    assert results["raw_orders"]["status"] == "completed"
    assert results["stg_orders"]["status"] == "failed"
    assert results["mart_revenue"]["status"] == "blocked"
    assert results["mart_revenue"]["blocked_by"] == "stg_orders"

    # Verify node statuses
    assert dag.get_node("stg_orders").status == "failed"
    assert dag.get_node("mart_revenue").status == "failed"
