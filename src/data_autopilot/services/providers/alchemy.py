from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)


class AlchemyProvider(BaseProvider):
    name = "alchemy"

    def __init__(self, api_key: str = "", base_url: str = "") -> None:
        url = base_url or f"https://eth-mainnet.g.alchemy.com/v2/{api_key}"
        super().__init__(api_key=api_key, base_url=url)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        dispatch = {
            "get_token_balances": self._get_token_balances,
            "get_asset_transfers": self._get_asset_transfers,
            "get_logs": self._get_logs,
        }
        handler = dispatch.get(method)
        if handler is None:
            return ProviderResult(
                provider=self.name, method=method, error=f"Unknown method: {method}"
            )
        try:
            return handler(params)
        except Exception as exc:
            logger.error("Alchemy %s failed: %s", method, exc, exc_info=True)
            return ProviderResult(provider=self.name, method=method, error=str(exc))

    def _get_token_balances(self, params: dict[str, Any]) -> ProviderResult:
        address = params.get("address", "")
        if not address:
            return ProviderResult(
                provider=self.name,
                method="get_token_balances",
                error="address required",
            )
        data = self._post_json_rpc(
            self.base_url, "alchemy_getTokenBalances", [address]
        )
        result = data.get("result", {})
        balances = result.get("tokenBalances", [])
        records = [
            {"contract": b.get("contractAddress", ""), "balance": b.get("tokenBalance", "0")}
            for b in balances
        ]
        return ProviderResult(
            provider=self.name,
            method="get_token_balances",
            records=records,
            total_available=len(records),
        )

    def _get_asset_transfers(self, params: dict[str, Any]) -> ProviderResult:
        address = params.get("address", "")
        if not address:
            return ProviderResult(
                provider=self.name,
                method="get_asset_transfers",
                error="address required",
            )

        all_records: list[dict[str, Any]] = []
        page_key: str | None = None

        while True:
            rpc_params: dict[str, Any] = {
                "fromAddress": address,
                "category": ["external", "erc20"],
                "maxCount": "0x64",
            }
            if page_key:
                rpc_params["pageKey"] = page_key

            data = self._post_json_rpc(
                self.base_url, "alchemy_getAssetTransfers", [rpc_params]
            )
            result = data.get("result", {})
            transfers = result.get("transfers", [])
            all_records.extend(transfers)
            page_key = result.get("pageKey")
            if not page_key or not transfers:
                break

        return ProviderResult(
            provider=self.name,
            method="get_asset_transfers",
            records=all_records,
            total_available=len(all_records),
        )

    def _get_logs(self, params: dict[str, Any]) -> ProviderResult:
        address = params.get("address", "")
        if not address:
            return ProviderResult(
                provider=self.name, method="get_logs", error="address required"
            )
        from_block = params.get("from_block", "latest")
        data = self._post_json_rpc(
            self.base_url,
            "eth_getLogs",
            [{"address": address, "fromBlock": str(from_block)}],
        )
        result = data.get("result", [])
        records = result if isinstance(result, list) else []
        return ProviderResult(
            provider=self.name,
            method="get_logs",
            records=records,
            total_available=len(records),
        )
