"""Phase 7 tests: Agent manager, RBAC, cross-source joins, end-to-end team flow."""

from unittest.mock import MagicMock

from data_autopilot.services.mode1.agent_manager import AgentManager
from data_autopilot.services.mode1.cross_source import CrossSourceJoin
from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.models import (
    AgentManagerConfig,
    TeamMember,
    TeamRole,
)
from data_autopilot.services.mode1.slack_handler import SlackEvent, SlackHandler


def test_agent_manager_set_aliases() -> None:
    """7.7: Data engineer configures table aliases → team members use in queries."""
    alias_mgr = EntityAliasManager()
    manager = AgentManager(alias_manager=alias_mgr)

    # Add engineer
    manager.add_member("org_am", TeamMember(
        user_id="eng_1", role=TeamRole.ENGINEER, name="Data Engineer",
    ))

    # Engineer sets aliases
    result = manager.set_aliases("org_am", "eng_1", {
        "fct_orders_v2_final": "Orders",
        "dim_users_current": "Users",
    })

    assert result["status"] == "success"
    assert result["aliases_set"] == 2

    # Verify aliases resolve
    assert alias_mgr.resolve("org_am", "Orders") == "fct_orders_v2_final"
    assert alias_mgr.resolve("org_am", "Users") == "dim_users_current"

    # Verify query text resolution
    table = alias_mgr.get_table_for_query("org_am", "Show me orders from last week")
    assert table == "fct_orders_v2_final"


def test_agent_manager_rbac() -> None:
    """7.8: Non-admin tries to modify contract → blocked."""
    manager = AgentManager()

    # Add viewer and admin
    manager.add_member("org_rbac", TeamMember(
        user_id="viewer_1", role=TeamRole.VIEWER, name="Viewer",
    ))
    manager.add_member("org_rbac", TeamMember(
        user_id="admin_1", role=TeamRole.ADMIN, name="Admin",
    ))
    manager.add_member("org_rbac", TeamMember(
        user_id="eng_1", role=TeamRole.ENGINEER, name="Engineer",
    ))

    # Viewer cannot modify contract
    result = manager.modify_contract("org_rbac", "viewer_1")
    assert result["status"] == "blocked"

    # Engineer cannot modify contract
    result = manager.modify_contract("org_rbac", "eng_1")
    assert result["status"] == "blocked"

    # Admin can modify contract
    result = manager.modify_contract("org_rbac", "admin_1")
    assert result["status"] == "allowed"

    # Viewer can query
    assert manager.check_permission("org_rbac", "viewer_1", "query")

    # Viewer cannot set aliases
    assert not manager.check_permission("org_rbac", "viewer_1", "set_alias")

    # Engineer can set aliases
    assert manager.check_permission("org_rbac", "eng_1", "set_alias")


def test_cross_source_join() -> None:
    """7.9: 'Which of our customers hold $TOKEN?' → joined warehouse + public."""
    joiner = CrossSourceJoin()

    # Public blockchain data: token holders
    public_data = [
        {"wallet": "wallet_1", "balance": 1000, "token": "BONK"},
        {"wallet": "wallet_2", "balance": 500, "token": "BONK"},
        {"wallet": "wallet_3", "balance": 2000, "token": "BONK"},
        {"wallet": "wallet_4", "balance": 100, "token": "BONK"},
    ]

    # Warehouse data: customers with linked wallets
    warehouse_data = [
        {"customer_id": "cust_1", "name": "Alice", "wallet": "wallet_1"},
        {"customer_id": "cust_2", "name": "Bob", "wallet": "wallet_3"},
        {"customer_id": "cust_3", "name": "Charlie", "wallet": "wallet_99"},  # No match
    ]

    result = joiner.join(
        public_data=public_data,
        warehouse_data=warehouse_data,
        join_key="wallet",
    )

    assert result.public_records == 4
    assert result.warehouse_records == 3
    assert result.joined_records == 2  # wallet_1 and wallet_3 match
    assert result.join_key == "wallet"

    # Verify joined data has both public and warehouse fields
    for record in result.records:
        assert "wallet" in record
        assert "balance" in record
        assert "customer_id" in record
        assert "name" in record


def test_cross_source_large_dataset() -> None:
    """7.10: Join warehouse (100K rows) + public (50K rows) via batch processing."""
    joiner = CrossSourceJoin()

    # Generate large datasets with 1% overlap
    public_data = [
        {"address": f"addr_{i}", "balance": i * 10}
        for i in range(50000)
    ]

    warehouse_data = [
        {"user_id": f"user_{i}", "address": f"addr_{i * 100}"}
        for i in range(100000)
    ]

    result = joiner.join_large(
        public_data=public_data,
        warehouse_data=warehouse_data,
        join_key="address",
        batch_size=10000,
    )

    assert result.public_records == 50000
    assert result.warehouse_records == 100000
    assert result.joined_records > 0
    # Matches: addr_0, addr_100, addr_200, ..., addr_49900 = 500 matches
    assert result.joined_records == 500


def test_e2e_team_flow() -> None:
    """7.11: Engineer connects DB → sets aliases → team member asks in Slack → correct answer."""

    # Step 1: Engineer configures agent
    alias_mgr = EntityAliasManager()
    manager = AgentManager(alias_manager=alias_mgr)

    manager.add_member("org_team", TeamMember(
        user_id="eng_1", role=TeamRole.ENGINEER, name="Data Engineer",
    ))
    manager.add_member("org_team", TeamMember(
        user_id="viewer_1", role=TeamRole.VIEWER, name="Team Member",
    ))

    manager.configure("org_team", AgentManagerConfig(
        allowed_schemas=["raw", "staging", "marts"],
        delivery_channels=["slack"],
    ))

    # Step 2: Engineer sets aliases
    result = manager.set_aliases("org_team", "eng_1", {
        "fct_orders_v2_final": "Orders",
    })
    assert result["status"] == "success"

    # Step 3: Team member asks a question in Slack
    mock_conv = MagicMock()
    mock_conv.respond.return_value = {
        "response_type": "business_result",
        "summary": "Found 1,234 orders from last week",
        "data": {"records": [{"count": 1234}], "record_count": 1},
        "warnings": [],
    }

    handler = SlackHandler(conversation_service=mock_conv)
    handler.register_workspace("T_TEAM", "org_team")

    event = SlackEvent(
        team_id="T_TEAM",
        channel="C_DATA",
        user="viewer_1",
        text="Show me orders from last week",
        ts="123.456",
    )

    slack_result = handler.handle_message(event)

    assert slack_result["status"] == "sent"
    assert "1,234" in slack_result["response"]

    # Verify viewer can query but not modify
    assert manager.check_permission("org_team", "viewer_1", "query")
    assert not manager.check_permission("org_team", "viewer_1", "set_alias")

    # Verify alias resolves for the query
    resolved = alias_mgr.get_table_for_query("org_team", "Show me orders from last week")
    assert resolved == "fct_orders_v2_final"
