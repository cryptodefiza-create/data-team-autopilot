from __future__ import annotations

import logging

from data_autopilot.services.mode1.models import DepthReport, FundingReport, VolumeReport
from data_autopilot.services.providers.binance import BinanceProvider

logger = logging.getLogger(__name__)


class CEXPublicData:
    """Public exchange data — no API keys needed."""

    def __init__(self, binance: BinanceProvider | None = None) -> None:
        self._binance = binance or BinanceProvider(mock_mode=True)

    def trading_volume(self, symbol: str, exchange: str = "binance") -> VolumeReport:
        """24h trading volume for a pair on a specific exchange."""
        if exchange != "binance":
            return VolumeReport(exchange=exchange, symbol=symbol)
        return self._binance.get_volume_report(symbol)

    def order_book_depth(self, symbol: str, exchange: str = "binance") -> DepthReport:
        """Order book snapshot — bid/ask spread, depth at various levels."""
        if exchange != "binance":
            return DepthReport(exchange=exchange, symbol=symbol)
        return self._binance.get_depth_report(symbol)

    def funding_rate(self, symbol: str) -> FundingReport:
        """Perpetual futures funding rate — sentiment indicator."""
        return self._binance.get_funding_report(symbol)
