"""Phase 4R tests: DeFi analytics — pool data, protocol revenue."""

from data_autopilot.services.mode1.defi_analytics import DeFiAnalytics
from data_autopilot.services.providers.defillama import DefiLlamaProvider
from data_autopilot.services.providers.dexscreener import DexScreenerProvider


def test_pool_analytics() -> None:
    """4.5: Show me the BONK/SOL pool on Raydium → TVL, volume, fees, APR."""
    dex = DexScreenerProvider(mock_mode=True)
    defi = DeFiAnalytics(dexscreener=dex)

    pool = defi.pool_analytics("bonk_sol_pool_address", protocol="raydium")

    assert pool.tvl == 5_000_000  # Mock default
    assert pool.volume_24h == 2_500_000
    assert pool.fees_24h > 0  # 0.3% of volume
    assert pool.fee_apr > 0
    assert pool.token_0 == "BONK"
    assert pool.token_1 == "SOL"
    assert pool.protocol == "raydium"


def test_protocol_revenue() -> None:
    """4.6: How much revenue does Aave generate? → daily fees, trend."""
    llama = DefiLlamaProvider(mock_mode=True)
    defi = DeFiAnalytics(defillama=llama)

    revenue = defi.protocol_revenue("aave", days=30)

    assert revenue.protocol_name == "aave"
    assert revenue.total_fees > 0
    assert revenue.protocol_revenue > 0
    assert len(revenue.daily_breakdown) == 30
    assert revenue.trend in ("increasing", "decreasing", "stable")
    assert revenue.period_days == 30
