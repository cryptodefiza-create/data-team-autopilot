"""Phase 4R tests: CEX data — public volume, funding rate, connected balance, read-only check."""

from data_autopilot.services.mode1.cex_connected import CEXConnected
from data_autopilot.services.mode1.cex_public import CEXPublicData
from data_autopilot.services.providers.binance import BinanceProvider


def test_cex_volume() -> None:
    """4.7: What's the trading volume for SOL on Binance?"""
    binance = BinanceProvider(mock_mode=True)
    cex = CEXPublicData(binance=binance)

    report = cex.trading_volume("SOLUSDT", exchange="binance")

    assert report.exchange == "binance"
    assert report.symbol == "SOLUSDT"
    assert report.volume_24h == 1_500_000  # Mock default
    assert report.quote_volume_24h == 225_000_000
    assert report.price_change_pct == 3.45
    assert report.high_24h == 155.20
    assert report.low_24h == 148.50


def test_cex_funding_rate() -> None:
    """4.8: What's the SOL perp funding rate?"""
    binance = BinanceProvider(mock_mode=True)
    cex = CEXPublicData(binance=binance)

    report = cex.funding_rate("SOLUSDT")

    assert report.symbol == "SOLUSDT"
    assert report.current_rate == 0.00035
    assert report.interpretation == "longs paying shorts"
    assert report.next_funding_time != ""


def test_cex_connected_balance() -> None:
    """4.11: Connect Binance read-only key → portfolio with all assets + USD values."""
    cex = CEXConnected(mock_mode=True)
    cex.register_mock_balances("org_cex", [
        {"asset": "SOL", "balance": 100},
        {"asset": "BTC", "balance": 0.5},
        {"asset": "USDT", "balance": 10_000},
        {"asset": "DOGE", "balance": 0},  # Zero balance, should be excluded
    ])
    cex.register_mock_prices({"SOL": 150.0, "BTC": 65_000.0, "USDT": 1.0})

    portfolio = cex.portfolio_balance("org_cex", exchange="binance")

    assert portfolio.exchange == "binance"
    assert len(portfolio.assets) == 3  # DOGE excluded (zero balance)
    assert portfolio.total_value_usd == 100 * 150 + 0.5 * 65_000 + 10_000 * 1.0
    # Should be sorted by value descending
    assert portfolio.assets[0].asset == "BTC"  # 32,500
    assert portfolio.assets[1].asset == "SOL"  # 15,000
    assert portfolio.assets[2].asset == "USDT"  # 10,000


def test_cex_read_only_check() -> None:
    """4.12: API key with trading permissions → rejected."""
    cex = CEXConnected(mock_mode=True)

    # Key with trading permissions — should be rejected
    result = cex.connect_exchange(
        org_id="org_unsafe",
        exchange="binance",
        api_key="test_key",
        api_secret="test_secret",
        permissions=["read", "trading", "withdrawal"],
    )

    assert result["status"] == "rejected"
    assert "read-only" in result["reason"].lower()
    assert "trading" in result["unsafe_permissions"]
    assert "withdrawal" in result["unsafe_permissions"]

    # Key with read-only — should be accepted
    result = cex.connect_exchange(
        org_id="org_safe",
        exchange="binance",
        api_key="test_key",
        api_secret="test_secret",
        permissions=["read"],
    )

    assert result["status"] == "connected"
    assert result["exchange"] == "binance"
