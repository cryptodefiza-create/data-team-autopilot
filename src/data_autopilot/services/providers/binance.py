from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult, VolumeReport, DepthReport, FundingReport
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_BINANCE_BASE = "https://api.binance.com"
_BINANCE_FUTURES = "https://fapi.binance.com"


class BinanceProvider(BaseProvider):
    """Binance public market data provider."""

    name = "binance"

    def __init__(self, api_key: str = "", mock_mode: bool = False) -> None:
        super().__init__(api_key=api_key, base_url=_BINANCE_BASE)
        self._mock_mode = mock_mode
        self._mock_data: dict[str, Any] = {}

    def register_mock_data(self, method: str, data: Any) -> None:
        self._mock_data[method] = data

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_24h_ticker": self._get_24h_ticker,
            "get_order_book": self._get_order_book,
            "get_funding_rate": self._get_funding_rate,
        })

    def _get_24h_ticker(self, params: dict[str, Any]) -> ProviderResult:
        symbol = params.get("symbol", "SOLUSDT")
        if self._mock_mode:
            data = self._mock_data.get("get_24h_ticker", {
                "symbol": symbol,
                "volume": "1500000",
                "quoteVolume": "225000000",
                "priceChangePercent": "3.45",
                "highPrice": "155.20",
                "lowPrice": "148.50",
                "lastPrice": "153.10",
            })
            return ProviderResult(
                provider=self.name, method="get_24h_ticker",
                records=[data], total_available=1,
            )
        data = self._get(f"{self.base_url}/api/v3/ticker/24hr", {"symbol": symbol})
        return ProviderResult(
            provider=self.name, method="get_24h_ticker",
            records=[data] if isinstance(data, dict) else [], total_available=1,
        )

    def _get_order_book(self, params: dict[str, Any]) -> ProviderResult:
        symbol = params.get("symbol", "SOLUSDT")
        limit = params.get("limit", 100)
        if self._mock_mode:
            bids = [{"price": 150.0 - i * 0.1, "qty": 100 + i * 10} for i in range(limit)]
            asks = [{"price": 150.1 + i * 0.1, "qty": 80 + i * 10} for i in range(limit)]
            return ProviderResult(
                provider=self.name, method="get_order_book",
                records=[{"bids": bids, "asks": asks, "symbol": symbol}],
                total_available=1,
            )
        data = self._get(f"{self.base_url}/api/v3/depth", {"symbol": symbol, "limit": limit})
        return ProviderResult(
            provider=self.name, method="get_order_book",
            records=[data] if isinstance(data, dict) else [], total_available=1,
        )

    def _get_funding_rate(self, params: dict[str, Any]) -> ProviderResult:
        symbol = params.get("symbol", "SOLUSDT")
        if self._mock_mode:
            data = self._mock_data.get("get_funding_rate", {
                "symbol": symbol,
                "lastFundingRate": "0.00035",
                "nextFundingTime": "1700000000000",
            })
            return ProviderResult(
                provider=self.name, method="get_funding_rate",
                records=[data], total_available=1,
            )
        data = self._get(
            f"{_BINANCE_FUTURES}/fapi/v1/premiumIndex", {"symbol": symbol}
        )
        return ProviderResult(
            provider=self.name, method="get_funding_rate",
            records=[data] if isinstance(data, dict) else [], total_available=1,
        )

    def get_volume_report(self, symbol: str) -> VolumeReport:
        result = self.fetch("get_24h_ticker", {"symbol": symbol})
        if result.error or not result.records:
            return VolumeReport(exchange="binance", symbol=symbol)
        d = result.records[0]
        return VolumeReport(
            exchange="binance",
            symbol=symbol,
            volume_24h=float(d.get("volume", 0)),
            quote_volume_24h=float(d.get("quoteVolume", 0)),
            price_change_pct=float(d.get("priceChangePercent", 0)),
            high_24h=float(d.get("highPrice", 0)),
            low_24h=float(d.get("lowPrice", 0)),
        )

    def get_depth_report(self, symbol: str, limit: int = 100) -> DepthReport:
        result = self.fetch("get_order_book", {"symbol": symbol, "limit": limit})
        if result.error or not result.records:
            return DepthReport(exchange="binance", symbol=symbol)
        book = result.records[0]
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = asks[0]["price"] if asks else 0.0
        bid_depth = sum(
            b["qty"] for b in bids
            if b["price"] > best_bid * 0.99
        ) if bids else 0.0
        ask_depth = sum(
            a["qty"] for a in asks
            if a["price"] < best_ask * 1.01
        ) if asks else 0.0
        return DepthReport(
            exchange="binance",
            symbol=symbol,
            bid_ask_spread=best_ask - best_bid,
            bid_depth_1pct=bid_depth,
            ask_depth_1pct=ask_depth,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def get_funding_report(self, symbol: str) -> FundingReport:
        result = self.fetch("get_funding_rate", {"symbol": symbol})
        if result.error or not result.records:
            return FundingReport(symbol=symbol)
        d = result.records[0]
        rate = float(d.get("lastFundingRate", 0))
        return FundingReport(
            symbol=symbol,
            current_rate=rate,
            next_funding_time=str(d.get("nextFundingTime", "")),
            interpretation="longs paying shorts" if rate > 0 else "shorts paying longs",
        )
