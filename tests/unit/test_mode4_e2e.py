"""Phase 4 tests: End-to-end connect → question → dashboard flow."""

from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.airbyte_client import AirbyteClient
from data_autopilot.services.mode1.business_query import BusinessQueryEngine
from data_autopilot.services.mode1.credential_flow import CredentialFlow
from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.dashboard_builder import DashboardBuilder
from data_autopilot.services.mode1.models import ProviderResult, SnapshotRecord
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.thin_contract import ThinContractManager


def test_e2e_connect_question_dashboard() -> None:
    """4.15: Connect Shopify → 'Show revenue trend' → 'Track weekly' → full flow works."""

    # Step 1: Connect Shopify
    vault = CredentialVault(mock_mode=True)
    contract_mgr = ThinContractManager()
    flow = CredentialFlow(vault=vault, contract_manager=contract_mgr)

    with patch("data_autopilot.services.mode1.credential_flow.ShopifyConnector") as MockConn:
        mock_instance = MagicMock()
        mock_instance.test_auth.return_value = True
        mock_instance.get_shop_info.return_value = {
            "name": "E2E Store",
            "iana_timezone": "UTC",
            "currency": "USD",
        }
        mock_instance.extract.return_value = iter([])
        MockConn.return_value = mock_instance

        connect_result = flow.connect_shopify(
            "org_e2e4", "e2e-store.myshopify.com", "shpat_e2e_token"
        )

    assert connect_result["status"] == "connected"

    # Step 2: Query business data
    engine = BusinessQueryEngine(vault=vault, contract_manager=contract_mgr)
    orders = [
        {"id": i, "total_price": f"{100 + i * 10}.00", "created_at": f"2025-03-{i + 1:02d}"}
        for i in range(7)
    ]

    with patch("data_autopilot.services.mode1.business_query.ShopifyConnector") as MockConn:
        mock_instance = MagicMock()
        mock_instance.fetch.return_value = ProviderResult(
            provider="shopify", method="get_orders",
            records=orders, total_available=7,
        )
        MockConn.return_value = mock_instance

        query_result = engine.query("org_e2e4", "Show me my orders")

    assert query_result["response_type"] == "business_result"
    assert query_result["data"]["record_count"] == 7

    # Step 3: Create Airbyte connection for recurring sync
    airbyte = AirbyteClient(mock_mode=True)
    conn_id = airbyte.create_connection(
        source_config={"source_type": "shopify"},
        destination_config={"destination_type": "neon_postgres"},
        schedule="0 6 * * 1",  # Weekly
    )
    sync = airbyte.trigger_sync(conn_id)
    assert sync.status == "completed"

    # Step 4: Build dashboard from stored data
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_e2e4", tier="pro")
    backend = persistence.get_storage("org_e2e4")

    from datetime import datetime, timezone
    for day in range(1, 8):
        snap = SnapshotRecord(
            source="shopify",
            entity="orders",
            query_params={"store": "e2e-store"},
            record_id=f"order_{day}",
            payload_hash=f"hash_{day}",
            payload={"id": day, "total_price": f"{100 + day * 10}.00"},
            ingested_at=datetime(2025, 3, day, 12, 0, tzinfo=timezone.utc),
        )
        backend.insert_snapshot(snap)

    builder = DashboardBuilder(persistence)
    dash_result = builder.create_from_snapshots("org_e2e4", "orders")
    assert dash_result["status"] == "success"
    assert dash_result["dashboard"]["total_snapshots"] == 7
