from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.models import ConnectedSource
from data_autopilot.services.mode1.thin_contract import ThinContractManager
from data_autopilot.services.providers.shopify import ShopifyConnector
from data_autopilot.services.providers.stripe_provider import StripeConnector

logger = logging.getLogger(__name__)


class CredentialFlow:
    """Guided credential collection flow for connecting data sources."""

    def __init__(
        self,
        vault: CredentialVault,
        contract_manager: ThinContractManager,
    ) -> None:
        self._vault = vault
        self._contracts = contract_manager
        self._connected: dict[str, ConnectedSource] = {}  # f"{org_id}:{source}"

    def connect_shopify(
        self, org_id: str, shop_domain: str, access_token: str
    ) -> dict[str, Any]:
        """Connect a Shopify store: validate → store → collect contract → return stats."""
        connector = ShopifyConnector(shop_domain=shop_domain, access_token=access_token)

        # Step 1: Validate credentials
        if not connector.test_auth():
            return {
                "status": "error",
                "message": "Invalid Shopify credentials. Please check your store domain and access token.",
            }

        # Step 2: Store credentials
        cred_id = self._vault.store(
            org_id, "shopify",
            {"shop_domain": shop_domain, "access_token": access_token},
        )

        # Step 3: Get shop info and collect thin contract
        shop_info = connector.get_shop_info()
        contract = self._contracts.collect_from_shopify(shop_info)
        self._contracts.store(org_id, "shopify", contract)

        # Step 4: Count available data
        stats = self._count_shopify_data(connector)

        # Step 5: Register connected source
        source = ConnectedSource(
            org_id=org_id,
            source="shopify",
            shop_domain=shop_domain,
            credential_id=cred_id,
            contract=contract,
            stats=stats,
        )
        self._connected[f"{org_id}:shopify"] = source

        return {
            "status": "connected",
            "source": "shopify",
            "shop_domain": shop_domain,
            "credential_id": cred_id,
            "stats": stats,
            "contract": contract.model_dump(),
            "message": (
                f"Connected to {shop_domain}. "
                f"Found: {stats.get('orders', 0):,} orders, "
                f"{stats.get('products', 0):,} products, "
                f"{stats.get('customers', 0):,} customers."
            ),
        }

    def connect_stripe(self, org_id: str, api_key: str) -> dict[str, Any]:
        """Connect a Stripe account: validate → store → collect contract → return stats."""
        connector = StripeConnector(api_key=api_key)

        # Step 1: Validate credentials
        if not connector.test_auth():
            return {
                "status": "error",
                "message": "Invalid Stripe API key. Please check your key and try again.",
            }

        # Step 2: Store credentials
        cred_id = self._vault.store(
            org_id, "stripe",
            {"api_key": api_key},
        )

        # Step 3: Get account info and collect thin contract
        account_info = connector.get_account_info()
        contract = self._contracts.collect_from_stripe(account_info)
        self._contracts.store(org_id, "stripe", contract)

        # Step 4: Register connected source
        source = ConnectedSource(
            org_id=org_id,
            source="stripe",
            credential_id=cred_id,
            contract=contract,
        )
        self._connected[f"{org_id}:stripe"] = source

        return {
            "status": "connected",
            "source": "stripe",
            "credential_id": cred_id,
            "contract": contract.model_dump(),
            "message": "Connected to Stripe account.",
        }

    def get_connected_source(self, org_id: str, source: str) -> ConnectedSource | None:
        return self._connected.get(f"{org_id}:{source}")

    def is_connected(self, org_id: str, source: str) -> bool:
        return f"{org_id}:{source}" in self._connected

    @staticmethod
    def _count_shopify_data(connector: ShopifyConnector) -> dict[str, int]:
        """Count records for each entity — for mock, returns 0s."""
        try:
            orders = sum(1 for _ in connector.extract("orders"))
            products = sum(1 for _ in connector.extract("products"))
            customers = sum(1 for _ in connector.extract("customers"))
            return {"orders": orders, "products": products, "customers": customers}
        except Exception:
            return {"orders": 0, "products": 0, "customers": 0}
