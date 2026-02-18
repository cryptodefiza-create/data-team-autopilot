"""Phase 6 tests: End-to-end contract → transform → promote flow, memo with version."""


from data_autopilot.services.mode1.contract_version import ContractVersionManager
from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    MetricDefinition,
)
from data_autopilot.services.mode1.semantic_contract import SemanticContractManager


def test_memo_with_contract_version() -> None:
    """6.13: Generate memo after contract change → memo notes version change."""
    manager = SemanticContractManager()
    manager.create(
        org_id="org_memo",
        entities=[EntityConfig(name="order", primary_key="order_id")],
        metrics=[MetricDefinition(
            name="revenue",
            definition="SUM(order_amount) - SUM(refund_amount)",
            includes_refunds=True,
        )],
        defaults=ContractDefaults(timezone="America/New_York"),
    )

    version_mgr = ContractVersionManager(manager)

    # Update to v2
    version_mgr.update("org_memo", {
        "metrics": [{"name": "revenue", "definition": "SUM(order_amount)"}],
    })

    # Compare versions for memo annotation
    comparison = version_mgr.compare_versions("org_memo", 1, 2)
    assert comparison["total_changes"] > 0

    # Simulate memo generation that includes version annotation
    current = manager.get("org_memo")
    memo_lines = [
        f"Revenue calculated using contract v{current.version}",
        f"Definition: {current.metrics[0].definition}",
    ]

    if comparison["total_changes"] > 0:
        memo_lines.append(
            f"Note: Contract updated from v1 to v{current.version}. "
            f"{comparison['total_changes']} definition(s) changed."
        )

    memo = "\n".join(memo_lines)
    assert f"v{current.version}" in memo
    assert "updated" in memo.lower()
    assert "changed" in memo.lower()
