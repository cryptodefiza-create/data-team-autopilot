"""Phase 3 tests: Persistence, RBAC, JSONB storage, dedup, tier gating."""

import hashlib
import json
from datetime import datetime, timezone

from data_autopilot.services.mode1.models import SnapshotRecord
from data_autopilot.services.mode1.persistence import (
    MockStorageBackend,
    PersistenceManager,
    RBACViolation,
    TierLimitError,
)


def test_neon_provisioning() -> None:
    """3.1: Neon project created with 4 schemas + agent role."""
    mgr = PersistenceManager(mock_mode=True)
    config = mgr.ensure_storage("org_1", tier="pro")

    assert config.type == "mock"
    assert config.project_id.startswith("mock-")
    assert len(config.schemas) == 4
    assert set(config.schemas) == {"raw", "staging", "marts", "analytics"}

    backend = mgr.get_storage("org_1")
    assert isinstance(backend, MockStorageBackend)
    assert backend.schemas_exist
    assert backend.has_agent_role


def test_rbac_write_to_raw_succeeds() -> None:
    """3.2: Agent writes to raw/ succeeds."""
    backend = MockStorageBackend()
    backend.create_schemas()
    backend.create_agent_role()
    # Should not raise
    backend.write_to_schema("raw", {"test": "data"})


def test_rbac_write_to_public_blocked() -> None:
    """3.3: Agent writes to public/ blocked by RBAC."""
    backend = MockStorageBackend()
    backend.create_schemas()
    backend.create_agent_role()
    try:
        backend.write_to_schema("public", {"test": "data"})
        assert False, "Should have raised RBACViolation"
    except RBACViolation as exc:
        assert "public" in str(exc)
        assert "Allowed" in str(exc)


def test_jsonb_storage_1000_records() -> None:
    """3.4: Store 1000 token holder records with correct metadata."""
    mgr = PersistenceManager(mock_mode=True)
    mgr.ensure_storage("org_store", tier="pro")

    records = [{"wallet": f"addr_{i}", "balance": 1000 - i} for i in range(1000)]
    stored = mgr.store_snapshot(
        org_id="org_store",
        source="helius",
        entity="token_holders",
        query_params={"mint": "BONK", "chain": "solana"},
        records=records,
    )
    assert stored == 1000

    backend = mgr.get_storage("org_store")
    count = backend.count_snapshots("token_holders")
    assert count == 1000

    # Verify payload hashes are unique
    hashes = set()
    for r in backend._records:
        assert r.payload_hash not in hashes
        hashes.add(r.payload_hash)


def test_dedup_no_duplicates() -> None:
    """3.5: Re-ingesting same data produces no duplicates."""
    mgr = PersistenceManager(mock_mode=True)
    mgr.ensure_storage("org_dedup", tier="pro")

    records = [{"wallet": "addr_1", "balance": 100}]
    mgr.store_snapshot("org_dedup", "helius", "token_holders", {"mint": "X"}, records)
    mgr.store_snapshot("org_dedup", "helius", "token_holders", {"mint": "X"}, records)

    backend = mgr.get_storage("org_dedup")
    assert backend.count_snapshots("token_holders") == 1


def test_tier_gating_free_persistence() -> None:
    """3.12: Free user says 'track this weekly' → blocked with upgrade prompt."""
    mgr = PersistenceManager(mock_mode=True)
    try:
        mgr.ensure_storage("org_free", tier="free")
        assert False, "Should have raised TierLimitError"
    except TierLimitError as exc:
        assert "Pro tier" in str(exc)
        assert not mgr.has_storage("org_free")


def test_tier_gating_pro_persistence() -> None:
    """3.13: Pro user says 'track this weekly' → pipeline created."""
    mgr = PersistenceManager(mock_mode=True)
    config = mgr.ensure_storage("org_pro", tier="pro")
    assert config is not None
    assert mgr.has_storage("org_pro")
