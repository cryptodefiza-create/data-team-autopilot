from __future__ import annotations

import logging
from typing import Any, Iterator

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_STRIPE_BASE = "https://api.stripe.com/v1"


class StripeConnector(BaseProvider):
    """Stripe API connector for extracting payment/subscription data."""

    name = "stripe"

    def __init__(self, api_key: str = "") -> None:
        super().__init__(api_key=api_key, base_url=_STRIPE_BASE)
        self._auth_headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    def test_auth(self) -> bool:
        """Verify credentials by fetching account info."""
        try:
            resp = self._client.get(
                f"{self.base_url}/account",
                headers=self._auth_headers,
            )
            return resp.status_code == 200
        except Exception as exc:
            logger.warning("Stripe auth test failed: %s", exc)
            return False

    def get_account_info(self) -> dict[str, Any]:
        """Fetch basic account info for connection confirmation."""
        try:
            resp = self._client.get(
                f"{self.base_url}/account",
                headers=self._auth_headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return {}

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        """Fetch data from Stripe by method name."""
        try:
            handler = {
                "get_charges": self._get_charges,
                "get_subscriptions": self._get_subscriptions,
                "get_invoices": self._get_invoices,
                "get_customers": self._get_customers,
                "get_payments": self._get_payments,
            }.get(method)

            if handler is None:
                return ProviderResult(
                    provider=self.name, method=method,
                    error=f"Unknown method: {method}",
                )

            records = list(handler(params))
            return ProviderResult(
                provider=self.name,
                method=method,
                records=records,
                total_available=len(records),
            )
        except Exception as exc:
            return ProviderResult(
                provider=self.name, method=method,
                error=str(exc),
            )

    def extract(self, entity: str, limit: int = 100) -> Iterator[dict[str, Any]]:
        """Paginated extraction for any Stripe list endpoint."""
        params: dict[str, Any] = {"limit": min(limit, 100)}
        has_more = True

        while has_more:
            try:
                resp = self._client.get(
                    f"{self.base_url}/{entity}",
                    headers=self._auth_headers,
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
                records = data.get("data", [])
                for record in records:
                    yield record

                has_more = data.get("has_more", False)
                if has_more and records:
                    params["starting_after"] = records[-1]["id"]
            except Exception as exc:
                logger.error("Stripe extract %s failed: %s", entity, exc)
                break

    def _get_charges(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("charges")

    def _get_subscriptions(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("subscriptions")

    def _get_invoices(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("invoices")

    def _get_customers(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("customers")

    def _get_payments(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("payment_intents")
