from __future__ import annotations

import logging
from typing import Any

import httpx

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_SOLANA_GATEWAY = "https://solana-gateway.moralis.io"


class MoralisProvider(BaseProvider):
    name = "moralis"

    def __init__(self, api_key: str = "", base_url: str = "") -> None:
        super().__init__(api_key=api_key, base_url=base_url or _SOLANA_GATEWAY)
        self._client = httpx.Client(timeout=30.0)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_token_accounts": self._get_top_holders,
        })

    def _get_top_holders(self, params: dict[str, Any]) -> ProviderResult:
        mint = params.get("mint", "")
        if not mint:
            return ProviderResult(
                provider=self.name,
                method="get_token_accounts",
                error="mint required",
            )

        limit = min(params.get("limit", 20), 100)
        url = f"{self.base_url}/token/mainnet/{mint}/top-holders"
        headers = {"X-API-Key": self.api_key, "Accept": "application/json"}

        try:
            resp = self._client.get(url, params={"limit": limit}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.error("Moralis top-holders failed for %s: %s", mint, exc)
            return ProviderResult(
                provider=self.name,
                method="get_token_accounts",
                error=f"Moralis API error: {exc}",
            )

        # Moralis response: { result: [...], totalSupply: "..." }
        # Each holder: { ownerAddress, balance, balanceFormatted, usdValue,
        #                percentageRelativeToTotalSupply, isContract }
        holders = data.get("result", []) if isinstance(data, dict) else data

        records = []
        for i, h in enumerate(holders):
            owner = h.get("ownerAddress", h.get("owner_address", ""))
            balance_formatted = h.get("balanceFormatted", "0")
            usd_value = h.get("usdValue", h.get("usd_value", 0))
            pct = h.get("percentageRelativeToTotalSupply",
                        h.get("percentage_relative_to_total_supply", 0))

            try:
                amount = round(float(balance_formatted))
            except (ValueError, TypeError):
                amount = 0

            records.append({
                "owner": owner,
                "mint": mint,
                "amount": amount,
                "usd_value": round(float(usd_value), 2) if usd_value else 0,
                "percentage": round(float(pct), 4) if pct else 0,
                "rank": i + 1,
            })

        return ProviderResult(
            provider=self.name,
            method="get_token_accounts",
            records=records,
            total_available=len(records),
        )
