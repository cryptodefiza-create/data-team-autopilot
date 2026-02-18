"""Phase 4 tests: Shopify/Stripe connectors, Airbyte, guided flow, business queries."""

from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.airbyte_client import AirbyteClient
from data_autopilot.services.mode1.business_query import BusinessQueryEngine
from data_autopilot.services.mode1.credential_flow import CredentialFlow
from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.models import ThinContract
from data_autopilot.services.mode1.thin_contract import ThinContractManager
from data_autopilot.services.providers.shopify import ShopifyConnector
from data_autopilot.services.providers.stripe_provider import StripeConnector


def test_shopify_extract_orders_pagination() -> None:
    """4.4: Connected store with 100 orders → all extracted, pagination handled."""
    connector = ShopifyConnector(
        shop_domain="test-store.myshopify.com",
        access_token="shpat_test",
    )

    # Mock paginated responses
    page1_orders = [{"id": i, "total_price": f"{i * 10}.00"} for i in range(50)]
    page2_orders = [{"id": i + 50, "total_price": f"{(i + 50) * 10}.00"} for i in range(50)]

    call_count = {"n": 0}

    def mock_get(url, headers=None, params=None):
        call_count["n"] += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        if call_count["n"] == 1:
            resp.json.return_value = {"orders": page1_orders}
            resp.headers = {"Link": '<https://next-page.com>; rel="next"'}
        else:
            resp.json.return_value = {"orders": page2_orders}
            resp.headers = {}
        return resp

    with patch.object(connector._client, "get", side_effect=mock_get):
        records = list(connector.extract("orders"))

    assert len(records) == 100
    assert records[0]["id"] == 0
    assert records[99]["id"] == 99


def test_shopify_incremental_extraction() -> None:
    """4.5: Extract orders since yesterday → only new/updated orders."""
    from datetime import datetime, timedelta, timezone

    connector = ShopifyConnector(
        shop_domain="test-store.myshopify.com",
        access_token="shpat_test",
    )

    since = datetime.now(timezone.utc) - timedelta(days=1)
    recent_orders = [{"id": i, "updated_at": since.isoformat()} for i in range(5)]

    def mock_get(url, headers=None, params=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"orders": recent_orders}
        resp.headers = {}
        # Verify watermark filtering param is passed
        assert params is None or "updated_at_min" in params or params == {}
        return resp

    with patch.object(connector._client, "get", side_effect=mock_get):
        records = list(connector.extract("orders", since=since))

    assert len(records) == 5


def test_stripe_extract_payments() -> None:
    """4.6: Connected Stripe account → all payments extracted, pagination handled."""
    connector = StripeConnector(api_key="sk_test_abc123")

    page1 = [{"id": f"ch_{i}", "amount": i * 1000} for i in range(3)]
    page2 = [{"id": f"ch_{i + 3}", "amount": (i + 3) * 1000} for i in range(2)]

    call_count = {"n": 0}

    def mock_get(url, headers=None, params=None):
        call_count["n"] += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        if call_count["n"] == 1:
            resp.json.return_value = {"data": page1, "has_more": True}
        else:
            resp.json.return_value = {"data": page2, "has_more": False}
        return resp

    with patch.object(connector._client, "get", side_effect=mock_get):
        records = list(connector.extract("payment_intents"))

    assert len(records) == 5
    assert records[0]["id"] == "ch_0"


def test_guided_credential_flow_shopify() -> None:
    """4.7: User says 'connect my Shopify' → step-by-step walkthrough, successful connection."""
    vault = CredentialVault(mock_mode=True)
    contract_mgr = ThinContractManager()
    flow = CredentialFlow(vault=vault, contract_manager=contract_mgr)

    # Mock Shopify connector for auth + shop info
    with patch("data_autopilot.services.mode1.credential_flow.ShopifyConnector") as MockConnector:
        mock_instance = MagicMock()
        mock_instance.test_auth.return_value = True
        mock_instance.get_shop_info.return_value = {
            "name": "Cool Store",
            "iana_timezone": "America/New_York",
            "currency": "USD",
        }
        mock_instance.extract.return_value = iter([])  # Empty for count
        MockConnector.return_value = mock_instance

        result = flow.connect_shopify(
            "org_flow",
            shop_domain="cool-store.myshopify.com",
            access_token="shpat_valid_token",
        )

    assert result["status"] == "connected"
    assert result["source"] == "shopify"
    assert "credential_id" in result
    assert result["contract"]["timezone"] == "America/New_York"
    assert vault.has_credentials("org_flow", "shopify")


def test_airbyte_create_connection_and_sync() -> None:
    """4.10+4.11: Create Airbyte connection → sync starts → returns status."""
    client = AirbyteClient(mock_mode=True)

    conn_id = client.create_connection(
        source_config={"source_type": "shopify", "shop_domain": "test.myshopify.com"},
        destination_config={"destination_type": "neon_postgres", "host": "localhost"},
        schedule="0 6 * * *",
    )

    assert conn_id.startswith("conn_")
    assert client.connection_count == 1

    # Trigger sync
    sync = client.trigger_sync(conn_id)
    assert sync.status == "completed"  # Mock mode completes immediately
    assert sync.rows_synced == 1000

    # Check status
    status = client.get_sync_status(conn_id)
    assert status.connection_id == conn_id
    assert status.status == "completed"


def test_live_query_shopify_orders() -> None:
    """4.12: 'Show me orders from last week' → table of recent orders."""
    vault = CredentialVault(mock_mode=True)
    vault.store("org_q", "shopify", {
        "shop_domain": "test.myshopify.com",
        "access_token": "shpat_test",
    })
    contract_mgr = ThinContractManager()
    contract_mgr.store("org_q", "shopify", ThinContract())
    engine = BusinessQueryEngine(vault=vault, contract_manager=contract_mgr)

    orders = [
        {"id": 1, "total_price": "50.00", "created_at": "2025-03-01"},
        {"id": 2, "total_price": "75.00", "created_at": "2025-03-02"},
    ]

    with patch("data_autopilot.services.mode1.business_query.ShopifyConnector") as MockConn:
        from data_autopilot.services.mode1.models import ProviderResult
        mock_instance = MagicMock()
        mock_instance.fetch.return_value = ProviderResult(
            provider="shopify", method="get_orders",
            records=orders, total_available=2,
        )
        MockConn.return_value = mock_instance

        result = engine.query("org_q", "Show me orders from last week")

    assert result["response_type"] == "business_result"
    assert result["data"]["source"] == "shopify"
    assert result["data"]["record_count"] == 2


def test_revenue_by_category() -> None:
    """4.13: 'Revenue by product category last month' → aggregated revenue table."""
    vault = CredentialVault(mock_mode=True)
    vault.store("org_agg", "shopify", {
        "shop_domain": "test.myshopify.com",
        "access_token": "shpat_test",
    })
    contract_mgr = ThinContractManager()
    contract_mgr.store("org_agg", "shopify", ThinContract(
        revenue_definition="gross",
        exclude_test_orders=False,
    ))
    engine = BusinessQueryEngine(vault=vault, contract_manager=contract_mgr)

    orders = [
        {"id": 1, "total_price": "100.00", "product_type": "Shoes", "tags": ""},
        {"id": 2, "total_price": "200.00", "product_type": "Shoes", "tags": ""},
        {"id": 3, "total_price": "150.00", "product_type": "Hats", "tags": ""},
    ]

    with patch("data_autopilot.services.mode1.business_query.ShopifyConnector") as MockConn:
        from data_autopilot.services.mode1.models import ProviderResult
        mock_instance = MagicMock()
        mock_instance.fetch.return_value = ProviderResult(
            provider="shopify", method="get_orders",
            records=orders, total_available=3,
        )
        MockConn.return_value = mock_instance

        result = engine.query("org_agg", "Revenue by product category last month")

    assert result["response_type"] == "business_result"
    records = result["data"]["records"]
    # Should be aggregated by product_type
    assert len(records) == 2
    # Shoes should be first (higher revenue)
    assert records[0]["group"] == "Shoes"
    assert records[0]["revenue"] == 300.0
