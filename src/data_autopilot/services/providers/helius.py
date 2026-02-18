from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)


class HeliusProvider(BaseProvider):
    name = "helius"

    def __init__(self, api_key: str = "", base_url: str = "") -> None:
        url = base_url or f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        super().__init__(api_key=api_key, base_url=url)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        dispatch = {
            "get_token_accounts": self._get_token_accounts,
            "get_asset": self._get_asset,
            "get_signatures": self._get_signatures,
        }
        handler = dispatch.get(method)
        if handler is None:
            return ProviderResult(
                provider=self.name, method=method, error=f"Unknown method: {method}"
            )
        try:
            return handler(params)
        except Exception as exc:
            logger.error("Helius %s failed: %s", method, exc, exc_info=True)
            return ProviderResult(provider=self.name, method=method, error=str(exc))

    def _get_token_accounts(self, params: dict[str, Any]) -> ProviderResult:
        address = params.get("address", "")
        mint = params.get("mint", "")
        rpc_params: dict[str, Any] = {}
        if mint:
            rpc_params = {"mint": mint}
        elif address:
            rpc_params = {"owner": address}
        else:
            return ProviderResult(
                provider=self.name,
                method="get_token_accounts",
                error="address or mint required",
            )

        all_records: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            call_params: dict[str, Any] = {**rpc_params, "limit": 100}
            if cursor:
                call_params["cursor"] = cursor

            data = self._post_json_rpc(
                self.base_url, "getTokenAccounts", call_params
            )
            result = data.get("result", {})
            items = result.get("token_accounts", [])
            all_records.extend(items)
            cursor = result.get("cursor")
            if not cursor or not items:
                break

        return ProviderResult(
            provider=self.name,
            method="get_token_accounts",
            records=all_records,
            total_available=len(all_records),
        )

    def _get_asset(self, params: dict[str, Any]) -> ProviderResult:
        asset_id = params.get("address", "") or params.get("asset_id", "")
        if not asset_id:
            return ProviderResult(
                provider=self.name, method="get_asset", error="address required"
            )
        data = self._post_json_rpc(self.base_url, "getAsset", {"id": asset_id})
        result = data.get("result", {})
        if result:
            return ProviderResult(
                provider=self.name,
                method="get_asset",
                records=[result],
                total_available=1,
            )
        error = data.get("error", {})
        return ProviderResult(
            provider=self.name,
            method="get_asset",
            error=str(error) if error else "No asset found",
        )

    def _get_signatures(self, params: dict[str, Any]) -> ProviderResult:
        address = params.get("address", "")
        if not address:
            return ProviderResult(
                provider=self.name, method="get_signatures", error="address required"
            )
        limit = min(params.get("limit", 100), 1000)
        data = self._post_json_rpc(
            self.base_url,
            "getSignaturesForAddress",
            [address, {"limit": limit}],
        )
        result = data.get("result", [])
        records = result if isinstance(result, list) else []
        return ProviderResult(
            provider=self.name,
            method="get_signatures",
            records=records,
            total_available=len(records),
        )
