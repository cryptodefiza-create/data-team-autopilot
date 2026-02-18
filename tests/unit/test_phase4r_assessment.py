"""Phase 4R tests: Assessment deliverables — DAO dashboard, DeFi dashboard, weekly memo."""

from data_autopilot.services.mode1.assessment_builder import AssessmentBuilder
from data_autopilot.services.mode1.cex_public import CEXPublicData
from data_autopilot.services.mode1.defi_analytics import DeFiAnalytics
from data_autopilot.services.mode1.onchain_analytics import OnChainAnalytics
from data_autopilot.services.mode1.wallet_labeler import KNOWN_EXCHANGE_WALLETS
from data_autopilot.services.providers.binance import BinanceProvider
from data_autopilot.services.providers.defillama import DefiLlamaProvider
from data_autopilot.services.providers.dexscreener import DexScreenerProvider
from data_autopilot.services.providers.snapshot_org import SnapshotProvider


def _build_analytics() -> OnChainAnalytics:
    analytics = OnChainAnalytics(mock_mode=True)
    exchange_addr = list(KNOWN_EXCHANGE_WALLETS.keys())[0]
    # Holders for whale tracking
    holders = [
        {"address": "whale_1", "balance": 50_000},
        {"address": "whale_2", "balance": 30_000},
    ] + [{"address": f"small_{i}", "balance": 100} for i in range(200)]
    analytics.register_mock_holders("BONK_MINT", holders)
    # Holders for overlap
    analytics.register_mock_holders("WIF_MINT", [
        {"address": f"small_{i}", "balance": 50} for i in range(50, 250)
    ])
    # Transfers for exchange flow
    analytics.register_mock_transfers("BONK_MINT", [
        {"from": "user_1", "to": exchange_addr, "amount": 5_000},
        {"from": exchange_addr, "to": "user_2", "amount": 10_000},
    ])
    return analytics


def _build_defi() -> DeFiAnalytics:
    return DeFiAnalytics(
        dexscreener=DexScreenerProvider(mock_mode=True),
        defillama=DefiLlamaProvider(mock_mode=True),
        snapshot=SnapshotProvider(mock_mode=True),
    )


def _build_cex() -> CEXPublicData:
    return CEXPublicData(binance=BinanceProvider(mock_mode=True))


def test_dao_assessment() -> None:
    """4.13: Full DAO token analysis → dashboard with holders, whales, exchange flow, trends."""
    builder = AssessmentBuilder(
        analytics=_build_analytics(),
        defi=_build_defi(),
        cex=_build_cex(),
    )

    report = builder.build_dao_assessment(
        org_id="org_dao_assess",
        mint="BONK_MINT",
        token_symbol="BONK",
        pool_address="bonk_sol_pool",
        compare_mint="WIF_MINT",
    )

    assert report.assessment_type == "dao"
    assert report.org_id == "org_dao_assess"

    # Should have panels for: whales, holder trend, exchange flow, overlap, DEX, CEX
    panel_titles = [p.title for p in report.panels]
    assert "Top Holders & Whale Tracker" in panel_titles
    assert "Exchange Flow (7 Days)" in panel_titles
    assert "Community Overlap Analysis" in panel_titles
    assert "DEX Liquidity & Volume" in panel_titles
    assert "CEX Trading Volume" in panel_titles

    # Whale panel should have data
    whale_panel = next(p for p in report.panels if "Whale" in p.title)
    assert whale_panel.data["total_whales"] == 2

    # Overlap panel should have data
    overlap_panel = next(p for p in report.panels if "Overlap" in p.title)
    assert overlap_panel.data["overlap_count"] > 0

    # Memo should be generated
    assert "BONK" in report.memo
    assert "Weekly Intelligence Memo" in report.memo


def test_defi_assessment() -> None:
    """4.14: Full protocol analysis → dashboard with TVL, fees, pools, governance."""
    builder = AssessmentBuilder(
        defi=_build_defi(),
        cex=_build_cex(),
    )

    report = builder.build_defi_assessment(
        org_id="org_defi_assess",
        protocol_name="aave",
        pool_addresses=["pool_1", "pool_2"],
        governance_space="aave.eth",
    )

    assert report.assessment_type == "defi"
    assert report.org_id == "org_defi_assess"

    panel_titles = [p.title for p in report.panels]
    assert "Protocol TVL Breakdown" in panel_titles
    assert "Fee & Revenue Analysis" in panel_titles
    assert "Top Pool Analytics" in panel_titles
    assert "Governance Activity" in panel_titles

    # TVL panel
    tvl_panel = next(p for p in report.panels if "TVL" in p.title)
    assert tvl_panel.data["tvl"] > 0

    # Revenue panel
    rev_panel = next(p for p in report.panels if "Revenue" in p.title)
    assert rev_panel.data["total_fees"] > 0

    # Pool panel
    pool_panel = next(p for p in report.panels if "Pool" in p.title)
    assert len(pool_panel.data["pools"]) == 2

    # Governance panel
    gov_panel = next(p for p in report.panels if "Governance" in p.title)
    assert gov_panel.data["active_proposals"] >= 0

    # Memo
    assert "aave" in report.memo
    assert "Weekly Intelligence Memo" in report.memo


def test_assessment_weekly_memo() -> None:
    """4.15: Generate sample memo from all data → formatted memo with KPI deltas, narrative."""
    builder = AssessmentBuilder(
        analytics=_build_analytics(),
        defi=_build_defi(),
        cex=_build_cex(),
    )

    report = builder.build_dao_assessment(
        org_id="org_memo",
        mint="BONK_MINT",
        token_symbol="BONK",
    )

    memo = report.memo
    assert len(memo) > 100  # Non-trivial memo

    # Check memo sections
    assert "# Weekly Intelligence Memo" in memo
    assert "## Top Holders" in memo
    assert "## Exchange Flow" in memo
    assert "## CEX Trading Volume" in memo

    # Check memo contains data
    assert "whale" in memo.lower()
    assert "24h volume" in memo.lower()

    # Verify numbers are cited
    assert "$" in memo or "%" in memo
