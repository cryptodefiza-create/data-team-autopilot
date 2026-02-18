from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.coingecko.com/api/v3"

# Common symbol → CoinGecko ID mappings
_SYMBOL_MAP: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BONK": "bonk",
    "PEPE": "pepe",
    "DOGE": "dogecoin",
    "USDC": "usd-coin",
    "USDT": "tether",
    "MATIC": "matic-network",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "ARB": "arbitrum",
    "OP": "optimism",
    "WIF": "dogwifcoin",
    "JUP": "jupiter-exchange-solana",
    "RAY": "raydium",
    "RNDR": "render-token",
}


def resolve_token_id(symbol: str) -> str:
    clean = symbol.upper().lstrip("$")
    if clean in _SYMBOL_MAP:
        return _SYMBOL_MAP[clean]
    return clean.lower()


class CoinGeckoProvider(BaseProvider):
    name = "coingecko"

    def __init__(self, api_key: str = "", base_url: str = _BASE_URL) -> None:
        super().__init__(api_key=api_key, base_url=base_url)

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        dispatch = {
            "get_price": self._get_price,
            "get_coin_info": self._get_coin_info,
            "get_price_history": self._get_price_history,
        }
        handler = dispatch.get(method)
        if handler is None:
            return ProviderResult(
                provider=self.name,
                method=method,
                error=f"Unknown method: {method}",
            )
        try:
            return handler(params)
        except Exception as exc:
            logger.error("CoinGecko %s failed: %s", method, exc, exc_info=True)
            return ProviderResult(
                provider=self.name, method=method, error=str(exc)
            )

    def _get_price(self, params: dict[str, Any]) -> ProviderResult:
        token = params.get("token", "")
        coin_id = resolve_token_id(token)
        data = self._get(
            f"{self.base_url}/simple/price",
            params={"ids": coin_id, "vs_currencies": "usd", "include_24hr_change": "true"},
        )
        if isinstance(data, dict) and coin_id in data:
            record = {"coin": coin_id, **data[coin_id]}
            return ProviderResult(
                provider=self.name, method="get_price", records=[record], total_available=1
            )
        return ProviderResult(
            provider=self.name, method="get_price", error=f"No data for {coin_id}"
        )

    def _get_coin_info(self, params: dict[str, Any]) -> ProviderResult:
        token = params.get("token", "")
        coin_id = resolve_token_id(token)
        data = self._get(f"{self.base_url}/coins/{coin_id}")
        if isinstance(data, dict) and "id" in data:
            record = {
                "id": data["id"],
                "symbol": data.get("symbol"),
                "name": data.get("name"),
                "market_cap_rank": data.get("market_cap_rank"),
                "market_data": data.get("market_data", {}),
            }
            return ProviderResult(
                provider=self.name, method="get_coin_info", records=[record], total_available=1
            )
        return ProviderResult(
            provider=self.name, method="get_coin_info", error=f"No info for {coin_id}"
        )

    def _get_price_history(self, params: dict[str, Any]) -> ProviderResult:
        token = params.get("token", "")
        days = params.get("days", 30)
        coin_id = resolve_token_id(token)
        data = self._get(
            f"{self.base_url}/coins/{coin_id}/market_chart",
            params={"vs_currency": "usd", "days": str(days)},
        )
        if isinstance(data, dict) and "prices" in data:
            records = [
                {"timestamp": ts, "price": price} for ts, price in data["prices"]
            ]
            return ProviderResult(
                provider=self.name,
                method="get_price_history",
                records=records,
                total_available=len(records),
            )
        return ProviderResult(
            provider=self.name, method="get_price_history", error=f"No history for {coin_id}"
        )
