from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.thin_contract import ThinContractManager
from data_autopilot.services.providers.shopify import ShopifyConnector
from data_autopilot.services.providers.stripe_provider import StripeConnector

logger = logging.getLogger(__name__)

# Keywords that trigger Shopify queries
_SHOPIFY_KEYWORDS = {
    "order", "orders", "product", "products", "customer", "customers",
    "shopify", "store", "shop", "revenue", "sales", "aov",
}

# Keywords that trigger Stripe queries
_STRIPE_KEYWORDS = {
    "payment", "payments", "charge", "charges", "subscription", "subscriptions",
    "invoice", "invoices", "stripe", "mrr", "churn",
}

# Aggregation keywords
_AGG_KEYWORDS = {
    "total", "sum", "average", "avg", "count", "by", "per", "group",
    "category", "categories",
}


class BusinessQueryEngine:
    """Answers business data questions from connected Shopify/Stripe sources."""

    def __init__(
        self,
        vault: CredentialVault,
        contract_manager: ThinContractManager,
    ) -> None:
        self._vault = vault
        self._contracts = contract_manager

    def query(self, org_id: str, message: str) -> dict[str, Any]:
        """Route a business question to the appropriate connector and return results."""
        text = message.lower()
        source = self._detect_source(text, org_id)

        if source is None:
            return {
                "response_type": "error",
                "summary": "No connected data source found. Use 'connect my Shopify' or 'connect my Stripe' first.",
                "data": {},
            }

        credentials = self._vault.retrieve(org_id, source)
        if credentials is None:
            return {
                "response_type": "error",
                "summary": f"No {source} credentials found. Please connect your {source} account.",
                "data": {},
            }

        contract = self._contracts.get(org_id, source)

        if source == "shopify":
            return self._query_shopify(org_id, message, credentials, contract)
        elif source == "stripe":
            return self._query_stripe(org_id, message, credentials, contract)

        return {"response_type": "error", "summary": f"Unsupported source: {source}", "data": {}}

    def _detect_source(self, text: str, org_id: str) -> str | None:
        """Detect which source the question is about."""
        shopify_score = sum(1 for kw in _SHOPIFY_KEYWORDS if kw in text)
        stripe_score = sum(1 for kw in _STRIPE_KEYWORDS if kw in text)

        if shopify_score > stripe_score and self._vault.has_credentials(org_id, "shopify"):
            return "shopify"
        if stripe_score > shopify_score and self._vault.has_credentials(org_id, "stripe"):
            return "stripe"
        # Fallback: return whichever is connected
        if self._vault.has_credentials(org_id, "shopify"):
            return "shopify"
        if self._vault.has_credentials(org_id, "stripe"):
            return "stripe"
        return None

    def _query_shopify(
        self,
        org_id: str,
        message: str,
        credentials: dict[str, Any],
        contract: Any,
    ) -> dict[str, Any]:
        """Execute a Shopify query based on the user's question."""
        connector = ShopifyConnector(
            shop_domain=credentials["shop_domain"],
            access_token=credentials["access_token"],
        )

        entity = self._detect_shopify_entity(message.lower())
        method = f"get_{entity}"

        result = connector.fetch(method, {})
        if result.error:
            return {
                "response_type": "error",
                "summary": f"Shopify query failed: {result.error}",
                "data": {},
            }

        records = result.records

        # Apply thin contract (revenue definition, test order exclusion)
        if contract and entity == "orders":
            records = self._contracts.apply_revenue_filter(records, contract)

        # Detect aggregation
        text = message.lower()
        if any(kw in text for kw in _AGG_KEYWORDS):
            agg_result = self._aggregate(records, text, entity)
            return {
                "response_type": "business_result",
                "summary": f"Aggregated {entity} from Shopify",
                "data": {
                    "source": "shopify",
                    "entity": entity,
                    "records": agg_result,
                    "record_count": len(agg_result),
                },
            }

        return {
            "response_type": "business_result",
            "summary": f"Retrieved {len(records)} {entity} from Shopify",
            "data": {
                "source": "shopify",
                "entity": entity,
                "records": records,
                "record_count": len(records),
            },
        }

    def _query_stripe(
        self,
        org_id: str,
        message: str,
        credentials: dict[str, Any],
        contract: Any,
    ) -> dict[str, Any]:
        """Execute a Stripe query based on the user's question."""
        connector = StripeConnector(api_key=credentials["api_key"])
        entity = self._detect_stripe_entity(message.lower())
        method = f"get_{entity}"

        result = connector.fetch(method, {})
        if result.error:
            return {
                "response_type": "error",
                "summary": f"Stripe query failed: {result.error}",
                "data": {},
            }

        return {
            "response_type": "business_result",
            "summary": f"Retrieved {len(result.records)} {entity} from Stripe",
            "data": {
                "source": "stripe",
                "entity": entity,
                "records": result.records,
                "record_count": len(result.records),
            },
        }

    @staticmethod
    def _detect_shopify_entity(text: str) -> str:
        if any(w in text for w in ("product", "products", "category", "categories")):
            return "products"
        if any(w in text for w in ("customer", "customers")):
            return "customers"
        return "orders"

    @staticmethod
    def _detect_stripe_entity(text: str) -> str:
        if any(w in text for w in ("subscription", "subscriptions", "mrr")):
            return "subscriptions"
        if any(w in text for w in ("invoice", "invoices")):
            return "invoices"
        if any(w in text for w in ("customer", "customers")):
            return "customers"
        return "payments"

    @staticmethod
    def _aggregate(
        records: list[dict[str, Any]], text: str, entity: str
    ) -> list[dict[str, Any]]:
        """Simple aggregation: group by product_type/category and sum revenue."""
        group_key = None
        if "category" in text or "product" in text:
            group_key = "product_type"
        elif "customer" in text:
            group_key = "customer_id"

        if group_key is None:
            total = sum(float(r.get("_revenue", r.get("total_price", 0)) or 0) for r in records)
            return [{"metric": f"total_{entity}", "value": total, "count": len(records)}]

        groups: dict[str, dict[str, Any]] = {}
        for r in records:
            key = str(r.get(group_key, "Other"))
            if key not in groups:
                groups[key] = {"group": key, "revenue": 0.0, "count": 0}
            groups[key]["revenue"] += float(r.get("_revenue", r.get("total_price", 0)) or 0)
            groups[key]["count"] += 1

        return sorted(groups.values(), key=lambda x: x["revenue"], reverse=True)
