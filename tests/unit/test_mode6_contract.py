"""Phase 6 tests: Semantic contracts, versioning, conversational builder, comparison."""

from data_autopilot.services.mode1.contract_builder import ConversationalContractBuilder
from data_autopilot.services.mode1.contract_version import ContractVersionManager
from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    MetricDefinition,
    SemanticContract,
)
from data_autopilot.services.mode1.semantic_contract import SemanticContractManager


def _make_contract(org_id: str = "org_1") -> SemanticContract:
    """Create a test contract."""
    manager = SemanticContractManager()
    return manager.create(
        org_id=org_id,
        entities=[
            EntityConfig(
                name="order",
                grain="one row per order",
                source_table="staging.stg_orders",
                primary_key="order_id",
            ),
            EntityConfig(
                name="customer",
                grain="one row per customer",
                source_table="staging.stg_customers",
                primary_key="customer_id",
            ),
        ],
        metrics=[
            MetricDefinition(
                name="revenue",
                definition="SUM(order_amount) - SUM(refund_amount)",
                includes_refunds=True,
            ),
        ],
        defaults=ContractDefaults(
            timezone="America/New_York",
            currency="USD",
            week_start="Monday",
        ),
    )


def test_contract_creation() -> None:
    """6.1: Full YAML-style contract → stored, versioned, retrievable."""
    manager = SemanticContractManager()
    contract = manager.create(
        org_id="org_c1",
        entities=[
            EntityConfig(
                name="order",
                grain="one row per order",
                source_table="staging.stg_orders",
                primary_key="order_id",
                dedup_strategy="latest by updated_at",
                exclusions=["test orders", "cancelled orders"],
            ),
        ],
        metrics=[
            MetricDefinition(
                name="revenue",
                definition="SUM(order_amount) - SUM(refund_amount)",
                includes_tax=False,
                includes_refunds=True,
            ),
        ],
        defaults=ContractDefaults(
            timezone="America/New_York",
            currency="USD",
            week_start="Monday",
        ),
    )

    assert contract.version == 1
    assert contract.org_id == "org_c1"
    assert len(contract.entities) == 1
    assert contract.entities[0].name == "order"
    assert contract.entities[0].exclusions == ["test orders", "cancelled orders"]
    assert len(contract.metrics) == 1
    assert contract.metrics[0].name == "revenue"
    assert contract.defaults.timezone == "America/New_York"

    # Retrievable
    stored = manager.get("org_c1")
    assert stored is not None
    assert stored.version == 1


def test_conversational_builder() -> None:
    """6.2: Agent asks questions, user answers → valid contract generated."""
    builder = ConversationalContractBuilder()

    # Start session
    result = builder.start_session("org_conv")
    assert result["status"] == "in_progress"
    assert "question" in result
    assert result["question"]["id"] == "grain"

    # Answer question 1: grain
    result = builder.answer("org_conv", "grain", "one_order")
    assert result["status"] == "in_progress"
    assert result["question"]["id"] == "revenue"

    # Answer question 2: revenue
    result = builder.answer("org_conv", "revenue", "net_after_refunds")
    assert result["status"] == "in_progress"
    assert result["question"]["id"] == "active_customer"

    # Answer question 3: active customer
    result = builder.answer("org_conv", "active_customer", "order_90d")
    assert result["status"] == "completed"
    assert "contract" in result

    contract = result["contract"]
    assert isinstance(contract, SemanticContract)
    assert contract.org_id == "org_conv"
    assert len(contract.metrics) == 1
    assert contract.metrics[0].name == "revenue"
    assert "refund" in contract.metrics[0].definition.lower()
    assert builder.is_complete("org_conv")


def test_contract_update_versioning() -> None:
    """6.3: Change revenue definition → new version created, old preserved."""
    manager = SemanticContractManager()
    contract = _make_contract("org_v")
    manager.store("org_v", contract)

    version_mgr = ContractVersionManager(manager)

    new_version = version_mgr.update("org_v", {
        "metrics": [{"name": "revenue", "definition": "SUM(order_amount)"}],
    })

    assert new_version == 2

    # Current contract is v2
    current = manager.get("org_v")
    assert current is not None
    assert current.version == 2
    assert current.metrics[0].definition == "SUM(order_amount)"

    # v1 is preserved
    v1 = version_mgr.get_version("org_v", 1)
    assert v1 is not None
    assert v1.metrics[0].definition == "SUM(order_amount) - SUM(refund_amount)"

    # Both versions accessible
    versions = version_mgr.list_versions("org_v")
    assert 1 in versions
    assert 2 in versions


def test_contract_rollback() -> None:
    """6.4: Rollback from v2 to v1 → alias points to v1 definitions."""
    manager = SemanticContractManager()
    contract = _make_contract("org_rb")
    manager.store("org_rb", contract)

    version_mgr = ContractVersionManager(manager)

    # Create v2
    version_mgr.update("org_rb", {
        "metrics": [{"name": "revenue", "definition": "SUM(order_amount)"}],
    })

    assert manager.get("org_rb").version == 2

    # Rollback to v1
    restored = version_mgr.rollback("org_rb")
    assert restored == 1

    current = manager.get("org_rb")
    assert current.version == 1
    assert "refund" in current.metrics[0].definition.lower()


def test_contract_version_comparison() -> None:
    """6.12: Compare v1 vs v2 revenue → both values returned with delta."""
    manager = SemanticContractManager()
    contract = _make_contract("org_cmp")
    manager.store("org_cmp", contract)

    version_mgr = ContractVersionManager(manager)

    # Create v2 with changed revenue definition
    version_mgr.update("org_cmp", {
        "metrics": [{"name": "revenue", "definition": "SUM(order_amount)"}],
    })

    comparison = version_mgr.compare_versions("org_cmp", 1, 2)
    assert comparison["total_changes"] > 0
    assert len(comparison["differences"]) > 0

    # Find the revenue metric change
    revenue_diff = next(
        (d for d in comparison["differences"] if d.get("name") == "revenue"),
        None,
    )
    assert revenue_diff is not None
    assert revenue_diff["type"] == "metric_changed"
    assert "refund" in revenue_diff["v1"].lower()
    assert "refund" not in revenue_diff["v2"].lower()
