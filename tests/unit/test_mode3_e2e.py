"""Phase 3 tests: End-to-end track → query history flow."""

from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.historical_query import HistoricalQuery
from data_autopilot.services.mode1.live_fetcher import LiveFetcher
from data_autopilot.services.mode1.models import (
    Chain,
    DataRequest,
    Entity,
    Intent,
    ProviderResult,
    RoutingDecision,
    RoutingMode,
)
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.snapshot_pipeline import SnapshotPipeline


def test_e2e_track_then_query_history() -> None:
    """3.17: 'Track $BONK daily' → wait → 'Show me holder trend' from stored snapshots."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_e2e", tier="pro")

    # Set up mock provider with holder data
    mock_provider = MagicMock()
    run_counter = {"n": 0}

    def fetch_side_effect(method, params):
        run_counter["n"] += 1
        return ProviderResult(
            provider="coingecko",
            method=method,
            records=[
                {"wallet": f"w{i}", "balance": 100 * run_counter["n"] + i}
                for i in range(5)
            ],
            total_available=5,
        )

    mock_provider.fetch.side_effect = fetch_side_effect

    fetcher = LiveFetcher(
        providers={"coingecko": mock_provider},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier="pro",
    )

    pipeline_svc = SnapshotPipeline(persistence=persistence, fetcher=fetcher)

    # Step 1: Track
    request = DataRequest(
        raw_message="Track $BONK holders daily",
        intent=Intent.TRACK,
        chain=Chain.SOLANA,
        entity=Entity.TOKEN_HOLDERS,
        token="BONK",
    )

    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="coingecko",
            method_name="get_price",
        )
        pipe = pipeline_svc.create("org_e2e", request, schedule="daily")

        # Step 2: Simulate second run (next day)
        pipeline_svc.run(pipe)

    assert pipe.run_count == 2

    # Step 3: Query history
    hq = HistoricalQuery(persistence)
    result = hq.get_trend("org_e2e", "token_holders")

    assert result["response_type"] == "blockchain_result"
    records = result["data"]["records"]
    # Should have records from both runs (dedup by record_id keeps latest per wallet,
    # but we have different balances so different hashes)
    assert len(records) > 0
    assert result["data"]["source"] == "stored_snapshots"

    # Verify records have ingested_at from stored snapshots
    for rec in records:
        assert "ingested_at" in rec
        assert "wallet" in rec
