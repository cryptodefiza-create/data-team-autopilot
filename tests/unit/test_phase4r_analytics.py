"""Phase 4R tests: On-chain analytics — holder history, whales, exchange flow, overlap."""

from datetime import datetime, timedelta, timezone

from data_autopilot.services.mode1.models import SnapshotRecord
from data_autopilot.services.mode1.onchain_analytics import OnChainAnalytics
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.wallet_labeler import (
    KNOWN_EXCHANGE_WALLETS,
)


def test_holder_history_trend() -> None:
    """4.1: Show me BONK holder trend over 30 days → daily holder count + distribution."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_4r", tier="pro")
    backend = persistence.get_storage("org_4r")

    # Insert snapshots across 3 recent days
    now = datetime.now(timezone.utc)
    for day_offset in range(3):
        dt = now - timedelta(days=2 - day_offset)  # 2 days ago, 1 day ago, today
        count = 15 + day_offset * 5  # 15, 20, 25
        for i in range(count):
            snap = SnapshotRecord(
                source="helius",
                entity="token_holders",
                query_params={"mint": "BONK_MINT"},
                record_id=f"holder_d{day_offset}_{i}",
                payload_hash=f"h{day_offset}_{i}",
                payload={"wallet": f"w{i}", "balance": 1000 + i * 100},
                ingested_at=dt,
            )
            backend.insert_snapshot(snap)

    analytics = OnChainAnalytics(persistence=persistence, mock_mode=True)
    history = analytics.holder_history("BONK_MINT", days=30)

    assert len(history) == 3
    # Holder count should be growing
    assert history[0].holder_count == 15
    assert history[1].holder_count == 20
    assert history[2].holder_count == 25
    assert history[0].total_supply > 0
    assert history[0].top10_pct > 0


def test_whale_tracker() -> None:
    """4.2: Who are the whales for $TOKEN? → top holders with %, activity labels."""
    analytics = OnChainAnalytics(mock_mode=True)

    # Register mock holders: 2 whales (>1% of supply) + many small holders
    holders = [
        {"address": "whale_1", "balance": 50_000},  # 50% of supply
        {"address": "whale_2", "balance": 30_000},  # 30% of supply
    ] + [
        {"address": f"small_{i}", "balance": 100}  # 0.1% each
        for i in range(200)
    ]
    analytics.register_mock_holders("TOKEN_MINT", holders)

    # Register transfers to classify activity
    analytics.register_mock_transfers("TOKEN_MINT", [
        {"from": "someone", "to": "whale_1", "amount": 5000},
        {"from": "someone", "to": "whale_1", "amount": 3000},
        {"from": "someone", "to": "whale_1", "amount": 2000},
        {"from": "someone", "to": "whale_1", "amount": 1000},
        # whale_1 has 4 incoming, 0 outgoing → accumulating
        {"from": "whale_2", "to": "someone", "amount": 5000},
        {"from": "whale_2", "to": "someone", "amount": 3000},
        {"from": "whale_2", "to": "someone", "amount": 2000},
        {"from": "whale_2", "to": "someone", "amount": 1000},
        # whale_2 has 0 incoming, 4 outgoing → distributing
    ])

    whales = analytics.whale_tracker("TOKEN_MINT", threshold_pct=1.0)

    assert len(whales) == 2
    assert whales[0].address == "whale_1"  # Sorted by balance desc
    assert whales[0].pct_supply > 40  # ~50%
    assert whales[0].recent_activity == "accumulating"
    assert whales[1].address == "whale_2"
    assert whales[1].recent_activity == "distributing"


def test_exchange_flow() -> None:
    """4.3: Are holders sending tokens to exchanges? → net flow report."""
    analytics = OnChainAnalytics(mock_mode=True)

    # Use a real exchange address from the known list
    exchange_addr = list(KNOWN_EXCHANGE_WALLETS.keys())[0]

    analytics.register_mock_transfers("TOKEN_MINT", [
        # Inflows TO exchange (selling pressure)
        {"from": "user_1", "to": exchange_addr, "amount": 10_000},
        {"from": "user_2", "to": exchange_addr, "amount": 5_000},
        # Outflows FROM exchange (accumulation)
        {"from": exchange_addr, "to": "user_3", "amount": 20_000},
    ])

    flow = analytics.exchange_flow("TOKEN_MINT", days=7)

    assert flow.inflow_volume == 15_000  # 10K + 5K going to exchange
    assert flow.outflow_volume == 20_000  # 20K leaving exchange
    assert flow.net_flow == 5_000  # Net outflow (accumulation signal)
    assert flow.interpretation == "net_outflow"
    assert flow.inflow_count == 2
    assert flow.outflow_count == 1


def test_wallet_overlap() -> None:
    """4.4: How much overlap between $TOKEN_A and $TOKEN_B holders?"""
    analytics = OnChainAnalytics(mock_mode=True)

    # Token A: 100 holders
    holders_a = [{"address": f"wallet_{i}"} for i in range(100)]
    # Token B: 80 holders, 30 overlap with A
    holders_b = [{"address": f"wallet_{i}"} for i in range(70, 150)]

    analytics.register_mock_holders("MINT_A", holders_a)
    analytics.register_mock_holders("MINT_B", holders_b)

    overlap = analytics.wallet_overlap("MINT_A", "MINT_B")

    assert overlap.token_a_holders == 100
    assert overlap.token_b_holders == 80
    assert overlap.overlap_count == 30  # wallets 70-99
    assert overlap.overlap_pct_a == 30.0  # 30/100
    assert overlap.overlap_pct_b == 37.5  # 30/80
    assert len(overlap.overlap_wallets) == 30
