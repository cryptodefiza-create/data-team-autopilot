from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterator

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_API_VERSION = "2024-01"


class ShopifyConnector(BaseProvider):
    """Shopify Admin API connector for extracting business data."""

    name = "shopify"

    def __init__(self, shop_domain: str = "", access_token: str = "") -> None:
        self.shop_domain = shop_domain
        self.access_token = access_token
        base_url = f"https://{shop_domain}/admin/api/{_API_VERSION}" if shop_domain else ""
        super().__init__(api_key=access_token, base_url=base_url)
        self._headers = {"X-Shopify-Access-Token": access_token} if access_token else {}

    def test_auth(self) -> bool:
        """Verify credentials by fetching shop info."""
        try:
            resp = self._client.get(
                f"{self.base_url}/shop.json",
                headers=self._headers,
            )
            return resp.status_code == 200
        except Exception as exc:
            logger.warning("Shopify auth test failed: %s", exc)
            return False

    def get_shop_info(self) -> dict[str, Any]:
        """Fetch basic shop info for connection confirmation."""
        try:
            data = self._get_shopify("shop.json")
            return data.get("shop", {})
        except Exception:
            logger.warning("Failed to fetch Shopify shop info", exc_info=True)
            return {}

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        """Fetch data from Shopify by method name."""
        try:
            handler = {
                "get_orders": self._get_orders,
                "get_products": self._get_products,
                "get_customers": self._get_customers,
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

    def extract(
        self, entity: str, since: datetime | None = None, limit: int = 250
    ) -> Iterator[dict[str, Any]]:
        """Generic paginated extraction for any Shopify entity."""
        params: dict[str, Any] = {"limit": min(limit, 250)}
        if since:
            params["updated_at_min"] = since.isoformat()

        url = f"{self.base_url}/{entity}.json"
        while url:
            try:
                resp = self._client.get(url, headers=self._headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                for record in data.get(entity, []):
                    yield record
                url = self._get_next_page(resp.headers.get("Link", ""))
                params = {}  # Only on first request
            except Exception as exc:
                logger.error("Shopify extract %s failed: %s", entity, exc, exc_info=True)
                break

    def _get_orders(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        since = params.get("since")
        since_dt = datetime.fromisoformat(since) if isinstance(since, str) else since
        yield from self.extract("orders", since=since_dt)

    def _get_products(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("products")

    def _get_customers(self, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from self.extract("customers")

    def _get_shopify(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        resp = self._client.get(
            f"{self.base_url}/{endpoint}",
            headers=self._headers,
            params=params,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _get_next_page(link_header: str) -> str | None:
        """Parse Shopify Link header for next page URL."""
        if not link_header:
            return None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                return url
        return None
