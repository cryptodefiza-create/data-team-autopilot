from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from data_autopilot.db.base import Base
from data_autopilot.models.entities import Tenant, WorkflowQueue
from data_autopilot.services.migration_runner import MigrationRunner


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine, SessionLocal()


def test_runner_returns_summary_and_checks_tenants() -> None:
    engine, db = _session()
    try:
        db.add(Tenant(id="org_m1", name="Org M1"))
        db.add(WorkflowQueue(id="q1", tenant_id="org_m1", workflow_type="memo", payload={}, status="queued", reason="x"))
        db.commit()

        summary = MigrationRunner(engine).run(db)
        assert isinstance(summary.created_tables, int)
        assert "org_m1" in summary.tenants_checked
        assert summary.errors == []
    finally:
        db.close()


def test_runner_adds_compat_columns_when_missing() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE workflow_queue ("
                "id VARCHAR(64) PRIMARY KEY, "
                "tenant_id VARCHAR(64), "
                "workflow_type VARCHAR(64), "
                "payload JSON, "
                "status VARCHAR(32), "
                "reason VARCHAR(255), "
                "created_at DATETIME, "
                "processed_at DATETIME)"
            )
        )

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    db = SessionLocal()
    try:
        summary = MigrationRunner(engine).run(db)
        cols = {c["name"] for c in inspect(engine).get_columns("workflow_queue")}
        assert "attempts" in cols
        assert "error_history" in cols
        assert any("workflow_queue.attempts" == c for c in summary.compatibility_changes)
        assert any("workflow_queue.error_history" == c for c in summary.compatibility_changes)
    finally:
        db.close()
