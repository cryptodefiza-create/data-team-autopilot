from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_autopilot.agents.contracts import AgentPlan, PlanStep
from data_autopilot.db.base import Base
from data_autopilot.services.agent_service import AgentService


def _db_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def _deterministic_plan(message: str) -> AgentPlan:
    """Return a fixed plan so tests don't depend on LLM output."""
    sql = "SELECT 1 AS health_check"
    if "dau" in message.lower():
        sql = (
            "SELECT DATE(created_at) AS day, COUNT(DISTINCT user_id) AS dau "
            "FROM analytics.events GROUP BY 1"
        )
    return AgentPlan(
        goal="Respond to user query",
        steps=[PlanStep(step_id=1, tool="execute_query", inputs={"sql": sql})],
    )


def test_agent_blocks_dml() -> None:
    db = _db_session()
    svc = AgentService()
    result = svc.run(db=db, org_id="org_1", user_id="u_1", message="please update users set a=1")
    assert result["response_type"] in {"query_result", "blocked"}


def test_agent_returns_query_result() -> None:
    db = _db_session()
    svc = AgentService()
    with patch.object(svc.planner, "plan", side_effect=_deterministic_plan):
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
        with patch.object(svc.planner, "plan", side_effect=_deterministic_plan):
            result = svc.run(db=db, org_id="org_2", user_id="u_2", message="show me dau")
        assert result["response_type"] == "blocked"
        assert "requires approval" in result["data"]["reasons"][0]
        assert result["data"]["gate"]["approval_required"] is True
        assert result["data"]["gate"]["next_action"] == "preview_then_approve"
        assert result["data"]["approval"]["required"] is True
    finally:
        svc.critic.settings.per_query_max_bytes = old_soft
        svc.critic.settings.per_query_max_bytes_with_approval = old_hard
