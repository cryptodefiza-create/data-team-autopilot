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
        # getProgramAccounts on large token mints needs more time
        import httpx
        self._client = httpx.Client(timeout=60.0)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_token_accounts": self._get_token_accounts,
            "get_asset": self._get_asset,
            "get_signatures": self._get_signatures,
        })

    def _get_token_accounts(self, params: dict[str, Any]) -> ProviderResult:
        mint = params.get("mint", "")
        address = params.get("address", "")

        if not mint and not address:
            return ProviderResult(
                provider=self.name,
                method="get_token_accounts",
                error="address or mint required",
            )

        # For mint queries (token holders): use getTokenLargestAccounts for top holders
        # then enrich with total holder count via fast pagination count
        if mint:
            return self._get_token_holders_by_mint(mint)

        # For owner queries: use getTokenAccounts
        return self._get_token_accounts_by_owner(address)

    def _get_token_holders_by_mint(self, mint: str) -> ProviderResult:
        # Step 1: Get top 20 holders instantly via native Solana RPC
        data = self._post_json_rpc(
            self.base_url, "getTokenLargestAccounts", [mint]
        )
        largest = data.get("result", {}).get("value", [])

        # Step 2: Get total holder count (active holders, excludes zero-balance)
        total_holders = self._count_token_holders(mint)

        # Step 3: Resolve token account addresses → wallet owner addresses
        token_account_addrs = [a.get("address", "") for a in largest if a.get("address")]
        owner_map = self._resolve_owners(token_account_addrs)

        records = []
        for i, account in enumerate(largest):
            token_account = account.get("address", "")
            wallet_owner = owner_map.get(token_account, token_account)
            # Use uiAmount (already decimal-adjusted by the RPC)
            ui_amount = account.get("uiAmount", 0)
            records.append({
                "token_account": token_account,
                "mint": mint,
                "owner": wallet_owner,
                "amount": round(ui_amount),
                "rank": i + 1,
                "total_holder_count": total_holders,
            })

        return ProviderResult(
            provider=self.name,
            method="get_token_accounts",
            records=records,
            total_available=total_holders or len(records),
        )

    def _count_token_holders(self, mint: str) -> int:
        """Count active (non-zero balance) holders via getProgramAccounts.

        Makes two RPC calls:
        1. Count ALL token accounts for this mint
        2. Count ZERO-BALANCE accounts (amount at offset 64 == 0)
        Active holders = total - zero_balance
        """
        _TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        _base_filters = [
            {"dataSize": 165},
            {"memcmp": {"offset": 0, "bytes": mint}},
        ]
        _slice = {"encoding": "base64", "dataSlice": {"offset": 0, "length": 0}}

        # Count all accounts
        total_data = self._post_json_rpc(
            self.base_url,
            "getProgramAccounts",
            [_TOKEN_PROGRAM, {**_slice, "filters": _base_filters}],
        )
        total_accounts = total_data.get("result")
        if not isinstance(total_accounts, list):
            return 0
        total = len(total_accounts)

        # Count zero-balance accounts (8 bytes of zero at offset 64 = "11111111" in base58)
        zero_data = self._post_json_rpc(
            self.base_url,
            "getProgramAccounts",
            [
                _TOKEN_PROGRAM,
                {
                    **_slice,
                    "filters": _base_filters + [
                        {"memcmp": {"offset": 64, "bytes": "11111111", "encoding": "base58"}},
                    ],
                },
            ],
        )
        zero_accounts = zero_data.get("result")
        zero_count = len(zero_accounts) if isinstance(zero_accounts, list) else 0

        active = total - zero_count
        logger.info("Holder count for %s: %d total, %d zero-balance, %d active", mint, total, zero_count, active)
        return active

    def _resolve_owners(self, token_accounts: list[str]) -> dict[str, str]:
        """Resolve SPL token account addresses → wallet owner addresses."""
        owner_map: dict[str, str] = {}
        if not token_accounts:
            return owner_map
        # Use getMultipleAccounts to batch-fetch all at once
        data = self._post_json_rpc(
            self.base_url,
            "getMultipleAccounts",
            [token_accounts, {"encoding": "jsonParsed"}],
        )
        accounts = data.get("result", {}).get("value", [])
        for addr, acct in zip(token_accounts, accounts):
            if acct and isinstance(acct, dict):
                parsed = acct.get("data", {}).get("parsed", {})
                info = parsed.get("info", {})
                wallet = info.get("owner", "")
                if wallet:
                    owner_map[addr] = wallet
        return owner_map

    def _get_token_decimals(self, mint: str) -> int:
        data = self._post_json_rpc(self.base_url, "getAsset", {"id": mint})
        result = data.get("result", {})
        return int(result.get("token_info", {}).get("decimals", 0))

    def _get_token_accounts_by_owner(self, address: str) -> ProviderResult:
        all_records: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            call_params: dict[str, Any] = {"owner": address, "limit": 1000}
            if cursor:
                call_params["cursor"] = cursor
            data = self._post_json_rpc(
                self.base_url, "getTokenAccounts", call_params
            )
            result = data.get("result", {})
            items = result.get("token_accounts", [])
            all_records.extend(items)
            cursor = result.get("cursor")
            if not cursor or not items or len(all_records) >= 1000:
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
