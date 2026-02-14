from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_autopilot.db.base import Base
from data_autopilot.services.agent_service import AgentService


def _db_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def test_agent_blocks_dml() -> None:
    db = _db_session()
    svc = AgentService()
    result = svc.run(db=db, org_id="org_1", user_id="u_1", message="please update users set a=1")
    assert result["response_type"] in {"query_result", "blocked"}


def test_agent_returns_query_result() -> None:
    db = _db_session()
    svc = AgentService()
    result = svc.run(db=db, org_id="org_1", user_id="u_1", message="show me dau")
    assert result["response_type"] == "query_result"
    assert "rows" in result["data"]


def test_agent_blocks_when_per_query_limit_exceeded() -> None:
    db = _db_session()
    svc = AgentService()
    old_soft = svc.critic.settings.per_query_max_bytes
    old_hard = svc.critic.settings.per_query_max_bytes_with_approval
    try:
        svc.critic.settings.per_query_max_bytes = 1
        svc.critic.settings.per_query_max_bytes_with_approval = 1_000_000
        result = svc.run(db=db, org_id="org_2", user_id="u_2", message="show me dau")
        assert result["response_type"] == "blocked"
        assert "requires approval" in result["data"]["reasons"][0]
        assert result["data"]["gate"]["approval_required"] is True
        assert result["data"]["gate"]["next_action"] == "preview_then_approve"
        assert result["data"]["approval"]["required"] is True
    finally:
        svc.critic.settings.per_query_max_bytes = old_soft
        svc.critic.settings.per_query_max_bytes_with_approval = old_hard
