from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ThinContract

logger = logging.getLogger(__name__)


class ThinContractManager:
    """Manages semantic contracts per org/source.

    A thin contract captures 3 business-level definitions that prevent
    'numbers don't match' problems:
    1. Revenue definition (gross vs net vs net-after-tax)
    2. Timezone (for day boundaries)
    3. Exclude test orders?
    """

    def __init__(self) -> None:
        self._contracts: dict[str, ThinContract] = {}  # f"{org_id}:{source}" -> contract

    def store(self, org_id: str, source: str, contract: ThinContract) -> None:
        key = f"{org_id}:{source}"
        self._contracts[key] = contract
        logger.info("Stored thin contract for %s", key)

    def get(self, org_id: str, source: str) -> ThinContract | None:
        return self._contracts.get(f"{org_id}:{source}")

    def has_contract(self, org_id: str, source: str) -> bool:
        return f"{org_id}:{source}" in self._contracts

    def apply_revenue_filter(
        self, records: list[dict[str, Any]], contract: ThinContract
    ) -> list[dict[str, Any]]:
        """Apply revenue definition to order/charge records."""
        filtered = records
        if contract.exclude_test_orders:
            filtered = [r for r in filtered if not _is_test_order(r)]

        for record in filtered:
            record["_revenue"] = _compute_revenue(record, contract.revenue_definition)

        return filtered

    def collect_from_shopify(self, shop_info: dict[str, Any]) -> ThinContract:
        """Auto-detect contract values from Shopify store settings."""
        tz = shop_info.get("iana_timezone", shop_info.get("timezone", "UTC"))
        currency = shop_info.get("currency", "USD")
        return ThinContract(
            revenue_definition="net_after_refunds",
            timezone=tz,
            exclude_test_orders=True,
            currency=currency,
        )

    def collect_from_stripe(self, account_info: dict[str, Any]) -> ThinContract:
        """Auto-detect contract values from Stripe account settings."""
        currency = account_info.get("default_currency", "usd").upper()
        country = account_info.get("country", "US")
        # US-based accounts default to America/New_York
        tz = "America/New_York" if country == "US" else "UTC"
        return ThinContract(
            revenue_definition="net_after_refunds",
            timezone=tz,
            exclude_test_orders=True,
            currency=currency,
        )


def _is_test_order(record: dict[str, Any]) -> bool:
    """Detect test orders by common signals."""
    if record.get("test", False):
        return True
    tags = str(record.get("tags", "")).lower()
    if "test" in tags:
        return True
    email = str(record.get("email", "")).lower()
    if email.endswith("@example.com") or "test" in email:
        return True
    return False


def _compute_revenue(record: dict[str, Any], definition: str) -> float:
    """Compute revenue based on the contract's definition."""
    # Try Shopify-style fields first, then Stripe-style
    total = float(record.get("total_price", record.get("amount", 0)) or 0)
    refunds = float(record.get("total_refunds", record.get("amount_refunded", 0)) or 0)
    tax = float(record.get("total_tax", record.get("tax", 0)) or 0)

    if definition == "gross":
        return total
    elif definition == "net_after_refunds":
        return total - refunds
    elif definition == "net_after_refunds_and_tax":
        return total - refunds - tax
    return total
