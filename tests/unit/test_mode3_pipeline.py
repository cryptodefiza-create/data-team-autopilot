"""Phase 3 tests: Pipeline creation, scheduling, failure handling, monitoring, cleanup."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.dashboard_builder import DashboardBuilder
from data_autopilot.services.mode1.historical_query import HistoricalQuery
from data_autopilot.services.mode1.live_fetcher import LiveFetcher
from data_autopilot.services.mode1.models import (
    Chain,
    DataRequest,
    Entity,
    Intent,
    Pipeline,
    PipelineStatus,
    ProviderResult,
    RoutingDecision,
    RoutingMode,
    SnapshotRecord,
)
from data_autopilot.services.mode1.neon_cleanup import NeonCleanup, NeonProject
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.pipeline_monitor import PipelineMonitor
from data_autopilot.services.mode1.pipeline_scheduler import PipelineScheduler
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.snapshot_pipeline import SnapshotPipeline


def _make_pipeline_env(records=None, should_fail=False):
    """Helper: set up persistence + fetcher + pipeline with mock provider."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_pipe", tier="pro")

    mock_provider = MagicMock()
    if should_fail:
        mock_provider.fetch.return_value = ProviderResult(
            provider="test", method="get_price", error="Provider error"
        )
    else:
        mock_provider.fetch.return_value = ProviderResult(
            provider="coingecko",
            method="get_price",
            records=records or [{"wallet": f"w{i}", "balance": 100 * i} for i in range(10)],
            total_available=10,
        )

    fetcher = LiveFetcher(
        providers={"coingecko": mock_provider},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier="pro",
    )
    pipeline = SnapshotPipeline(persistence=persistence, fetcher=fetcher)
    return persistence, fetcher, pipeline


def test_pipeline_creation() -> None:
    """3.6: 'Track $BONK holders daily' creates pipeline, first run executes."""
    persistence, fetcher, pipeline_svc = _make_pipeline_env()

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
        pipe = pipeline_svc.create("org_pipe", request, schedule="daily")

    assert pipe.status == PipelineStatus.ACTIVE
    assert pipe.run_count == 1
    assert pipe.last_success is not None

    backend = persistence.get_storage("org_pipe")
    assert backend.count_snapshots() > 0


def test_pipeline_scheduling() -> None:
    """3.7: Daily pipeline runs at scheduled time, second run adds records."""
    persistence, fetcher, pipeline_svc = _make_pipeline_env()

    request = DataRequest(
        raw_message="Track price",
        intent=Intent.TRACK,
        entity=Entity.TOKEN_PRICE,
        token="ETH",
    )

    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="coingecko",
            method_name="get_price",
        )
        pipe = pipeline_svc.create("org_pipe", request, schedule="daily")
        count_after_first = persistence.get_storage("org_pipe").count_snapshots()

        # Change data for second run
        fetcher._providers["coingecko"].fetch.return_value = ProviderResult(
            provider="coingecko",
            method="get_price",
            records=[{"wallet": f"new_w{i}", "balance": 200 * i} for i in range(5)],
            total_available=5,
        )
        pipeline_svc.run(pipe)

    assert pipe.run_count == 2
    count_after_second = persistence.get_storage("org_pipe").count_snapshots()
    assert count_after_second > count_after_first


def test_pipeline_failure_handling() -> None:
    """3.8: Provider error → retries 3x, then marks as failed."""
    _, _, pipeline_svc = _make_pipeline_env(should_fail=True)

    request = DataRequest(
        raw_message="Track failing",
        intent=Intent.TRACK,
        entity=Entity.TOKEN_PRICE,
        token="FAIL",
    )

    pipe = Pipeline(
        id="pipe_fail_test",
        org_id="org_pipe",
        entity="token_price",
        chain="cross_chain",
        token="FAIL",
        schedule="daily",
    )
    pipeline_svc._pipelines[pipe.id] = pipe

    with patch.object(pipeline_svc._fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="coingecko",
            method_name="get_price",
        )
        success = pipeline_svc.run(pipe)

    assert not success
    assert pipe.status == PipelineStatus.FAILED
    assert pipe.last_error is not None


def test_pipeline_health_stale() -> None:
    """3.9: Pipeline not synced in 72 hours → status=stale with alert."""
    monitor = PipelineMonitor()
    now = datetime.now(timezone.utc)
    pipe = Pipeline(
        id="pipe_stale",
        org_id="org_1",
        entity="token_holders",
        schedule="daily",
        status=PipelineStatus.ACTIVE,
        last_run=now - timedelta(hours=72),
        last_success=now - timedelta(hours=72),
    )
    results = monitor.check_health([pipe])
    assert len(results) == 1
    assert results[0].status == PipelineStatus.STALE
    assert results[0].alert is not None
    assert "72" in results[0].alert


def test_historical_query_with_data() -> None:
    """3.10: 'Holders as of Feb 20' returns snapshot closest to date."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_hist", tier="pro")

    # Store snapshots at different times
    backend = persistence.get_storage("org_hist")
    for day in range(18, 22):
        snap = SnapshotRecord(
            source="helius",
            entity="token_holders",
            query_params={"mint": "BONK"},
            record_id=f"holder_{day}",
            payload_hash=f"hash_{day}",
            payload={"wallet": f"w{day}", "balance": day * 100},
            ingested_at=datetime(2025, 2, day, 12, 0, tzinfo=timezone.utc),
        )
        backend.insert_snapshot(snap)

    hq = HistoricalQuery(persistence)
    result = hq.query_snapshot(
        org_id="org_hist",
        entity="token_holders",
        as_of=datetime(2025, 2, 20, 23, 59, tzinfo=timezone.utc),
    )
    assert result["response_type"] == "blockchain_result"
    # Should include holders ingested on/before Feb 20
    assert len(result["data"]["records"]) == 3  # days 18, 19, 20


def test_historical_query_before_tracking() -> None:
    """3.11: 'Holders as of Jan 1' when tracking started Feb 18 → honest response."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_hist2", tier="pro")

    backend = persistence.get_storage("org_hist2")
    snap = SnapshotRecord(
        source="helius",
        entity="token_holders",
        query_params={"mint": "BONK"},
        record_id="holder_1",
        payload_hash="hash_1",
        payload={"wallet": "w1", "balance": 100},
        ingested_at=datetime(2025, 2, 18, 12, 0, tzinfo=timezone.utc),
    )
    backend.insert_snapshot(snap)

    hq = HistoricalQuery(persistence)
    result = hq.query_snapshot(
        org_id="org_hist2",
        entity="token_holders",
        as_of=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
    )
    assert result["response_type"] == "info"
    assert "tracking" in result["summary"].lower() or "after" in result["summary"].lower()


def test_neon_cleanup_inactive_pro() -> None:
    """3.14: Pro project inactive 15 days → project suspended."""
    now = datetime.now(timezone.utc)
    project = NeonProject(
        project_id="proj_1",
        org_id="org_1",
        last_activity=now - timedelta(days=15),
        tier="pro",
    )

    cleanup = NeonCleanup()
    result = cleanup.run_cleanup([project], now=now)
    assert "proj_1" in result["suspended"]
    assert project.suspended


def test_neon_cleanup_inactive_90_days() -> None:
    """3.15: Any project inactive 91 days → warning sent."""
    now = datetime.now(timezone.utc)
    project = NeonProject(
        project_id="proj_2",
        org_id="org_2",
        last_activity=now - timedelta(days=91),
        tier="pro",
    )

    cleanup = NeonCleanup()
    result = cleanup.run_cleanup([project], now=now)
    assert "proj_2" in result["warned"]
    assert len(cleanup.notifications) == 1
    assert cleanup.notifications[0]["type"] == "deletion_warning"


def test_dashboard_from_tracked_data() -> None:
    """3.16: 7 days of daily snapshots → dashboard with trend chart."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_dash", tier="pro")

    backend = persistence.get_storage("org_dash")
    for day in range(1, 8):
        for i in range(5):
            snap = SnapshotRecord(
                source="helius",
                entity="token_holders",
                query_params={"mint": "BONK"},
                record_id=f"holder_{day}_{i}",
                payload_hash=f"hash_{day}_{i}",
                payload={"wallet": f"w{i}", "balance": day * 100 + i},
                ingested_at=datetime(2025, 3, day, 12, 0, tzinfo=timezone.utc),
            )
            backend.insert_snapshot(snap)

    builder = DashboardBuilder(persistence)
    result = builder.create_from_snapshots("org_dash", "token_holders")
    assert result["status"] == "success"
    dashboard = result["dashboard"]
    assert "charts" in dashboard
    assert len(dashboard["charts"]) == 1
    assert dashboard["charts"][0]["type"] == "line"
    assert dashboard["total_snapshots"] == 35  # 7 days * 5 records
    assert dashboard["date_range"]["start"] is not None
