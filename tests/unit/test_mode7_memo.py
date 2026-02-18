"""Phase 7 tests: Weekly memo generation, stale data blocking, contract versioning."""

from datetime import datetime, timedelta, timezone

from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    MartTable,
    MetricDefinition,
    Pipeline,
    PipelineStatus,
    SemanticContract,
)
from data_autopilot.services.mode1.weekly_memo import WeeklyMemoScheduler


def _make_fresh_pipelines(now: datetime) -> list[Pipeline]:
    """Create pipelines with recent sync times."""
    return [
        Pipeline(
            id="pipe_1",
            org_id="org_memo",
            entity="orders",
            schedule="daily",
            status=PipelineStatus.ACTIVE,
            last_run=now - timedelta(hours=6),
            last_success=now - timedelta(hours=6),
            run_count=10,
        ),
    ]


def _make_stale_pipelines(now: datetime) -> list[Pipeline]:
    """Create pipelines that haven't synced in 72 hours."""
    return [
        Pipeline(
            id="pipe_stale",
            org_id="org_memo",
            entity="orders",
            schedule="daily",
            status=PipelineStatus.ACTIVE,
            last_run=now - timedelta(hours=72),
            last_success=now - timedelta(hours=72),
            run_count=5,
        ),
    ]


def _make_contract(version: int = 1) -> SemanticContract:
    return SemanticContract(
        org_id="org_memo",
        version=version,
        entities=[EntityConfig(name="order", primary_key="order_id")],
        metrics=[MetricDefinition(
            name="revenue",
            definition="SUM(amount) - SUM(refund_amount)",
        )],
        defaults=ContractDefaults(timezone="America/New_York"),
    )


def _make_marts() -> dict[str, MartTable]:
    return {
        "mart_revenue": MartTable(
            name="mart_revenue",
            source_entities=["order"],
            row_count=100,
            columns=["order_id", "amount", "_revenue"],
            records=[
                {"order_id": f"ord_{i}", "amount": 50 + i, "_revenue": 45 + i}
                for i in range(100)
            ],
        ),
    }


def test_weekly_memo_generation() -> None:
    """7.4: Monday 6 AM trigger → memo generated with KPI deltas."""
    now = datetime(2025, 3, 10, 6, 0, tzinfo=timezone.utc)  # Monday 6 AM

    scheduler = WeeklyMemoScheduler()
    contract = _make_contract()

    result = scheduler.generate_memo(
        org_id="org_memo",
        pipelines=_make_fresh_pipelines(now),
        marts=_make_marts(),
        contract=contract,
        now=now,
    )

    assert result["status"] == "generated"
    memo = result["memo"]
    assert memo.org_id == "org_memo"
    assert memo.contract_version == 1
    assert "revenue" in memo.kpis
    assert memo.kpis["revenue"]["total"] > 0
    assert memo.kpis["revenue"]["count"] == 100
    assert "Weekly Data Summary" in memo.narrative
    assert "contract v1" in memo.narrative


def test_weekly_memo_stale_data_block() -> None:
    """7.5: Pipeline stale 72 hours → memo delayed, user notified."""
    now = datetime(2025, 3, 10, 6, 0, tzinfo=timezone.utc)

    scheduler = WeeklyMemoScheduler()

    result = scheduler.generate_memo(
        org_id="org_memo",
        pipelines=_make_stale_pipelines(now),
        marts=_make_marts(),
        contract=_make_contract(),
        now=now,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "stale_data"
    assert "stale" in result["message"].lower()
    assert "pipe_stale" in result["stale_pipelines"]


def test_weekly_memo_contract_version() -> None:
    """7.6: Memo after contract update → memo notes version change."""
    now = datetime(2025, 3, 10, 6, 0, tzinfo=timezone.utc)

    scheduler = WeeklyMemoScheduler()
    contract_v2 = _make_contract(version=2)

    result = scheduler.generate_memo(
        org_id="org_memo",
        pipelines=_make_fresh_pipelines(now),
        marts=_make_marts(),
        contract=contract_v2,
        now=now,
    )

    assert result["status"] == "generated"
    memo = result["memo"]
    assert memo.contract_version == 2
    assert "v2" in memo.narrative
