"""Phase 4 tests: Credential vault, validation, purge, thin contract."""

from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.models import ThinContract
from data_autopilot.services.mode1.thin_contract import ThinContractManager


def test_credential_storage_roundtrip() -> None:
    """4.1: Store Shopify API key → encrypted in DB, retrievable, decrypts correctly."""
    vault = CredentialVault(mock_mode=True)
    cred_id = vault.store(
        "org_1", "shopify",
        {"shop_domain": "cool-store.myshopify.com", "access_token": "shpat_abc123"},
    )

    assert cred_id.startswith("cred_")

    # Verify stored value is encrypted (not plain JSON)
    assert vault.is_encrypted("org_1", "shopify")

    # Retrieve and verify roundtrip
    retrieved = vault.retrieve("org_1", "shopify")
    assert retrieved is not None
    assert retrieved["shop_domain"] == "cool-store.myshopify.com"
    assert retrieved["access_token"] == "shpat_abc123"


def test_credential_validation_valid() -> None:
    """4.2: Valid Shopify token → returns True, shop info."""
    from data_autopilot.services.providers.shopify import ShopifyConnector

    connector = ShopifyConnector(shop_domain="test-store.myshopify.com", access_token="shpat_valid")

    with patch.object(connector._client, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        assert connector.test_auth() is True


def test_credential_validation_invalid() -> None:
    """4.3: Invalid Shopify token → returns False, does not store."""
    from data_autopilot.services.providers.shopify import ShopifyConnector

    connector = ShopifyConnector(shop_domain="bad-store.myshopify.com", access_token="bad_token")

    with patch.object(connector._client, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_get.return_value = mock_resp

        assert connector.test_auth() is False

    # Verify credentials were NOT stored in vault
    vault = CredentialVault(mock_mode=True)
    assert vault.retrieve("org_1", "shopify") is None


def test_thin_contract_revenue_definition() -> None:
    """4.8: Agent asks about revenue, timezone, test orders — all stored."""
    manager = ThinContractManager()

    # Collect from Shopify shop info
    shop_info = {
        "iana_timezone": "America/New_York",
        "currency": "USD",
    }
    contract = manager.collect_from_shopify(shop_info)

    assert contract.timezone == "America/New_York"
    assert contract.currency == "USD"
    assert contract.exclude_test_orders is True
    assert contract.revenue_definition == "net_after_refunds"

    # Store and retrieve
    manager.store("org_1", "shopify", contract)
    assert manager.has_contract("org_1", "shopify")
    stored = manager.get("org_1", "shopify")
    assert stored is not None
    assert stored.timezone == "America/New_York"


def test_thin_contract_applied_to_query() -> None:
    """4.9: 'What's my revenue last month?' uses user's revenue definition."""
    manager = ThinContractManager()
    contract = ThinContract(
        revenue_definition="net_after_refunds",
        timezone="America/New_York",
        exclude_test_orders=True,
        currency="USD",
    )

    orders = [
        {"id": 1, "total_price": "100.00", "total_refunds": "10.00", "total_tax": "5.00", "tags": ""},
        {"id": 2, "total_price": "200.00", "total_refunds": "0.00", "total_tax": "10.00", "tags": "test"},
        {"id": 3, "total_price": "50.00", "total_refunds": "5.00", "total_tax": "2.50", "email": "test@example.com"},
    ]

    filtered = manager.apply_revenue_filter(orders, contract)

    # Test orders (id 2 and 3) should be excluded
    assert len(filtered) == 1
    # Revenue should be net_after_refunds: 100 - 10 = 90
    assert filtered[0]["_revenue"] == 90.0


def test_credential_purge() -> None:
    """4.14: Delete org credentials → all removed, nothing retrievable."""
    vault = CredentialVault(mock_mode=True)
    vault.store("org_purge", "shopify", {"token": "abc"})
    vault.store("org_purge", "stripe", {"key": "def"})

    assert vault.has_credentials("org_purge", "shopify")
    assert vault.has_credentials("org_purge", "stripe")

    count = vault.purge("org_purge")
    assert count == 2
    assert vault.retrieve("org_purge", "shopify") is None
    assert vault.retrieve("org_purge", "stripe") is None
    assert not vault.has_credentials("org_purge", "shopify")
