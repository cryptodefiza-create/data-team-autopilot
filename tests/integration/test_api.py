from unittest.mock import patch

from fastapi.testclient import TestClient

from data_autopilot.agents.contracts import AgentPlan, PlanStep
from data_autopilot.main import app
from data_autopilot.api.state import agent_service


client = TestClient(app)


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


def test_health() -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_agent_run() -> None:
    org = "org_1"
    payload = {
        "org_id": org,
        "user_id": "user_1",
        "message": "show me dau",
        "session_id": "sess_1",
    }
    with patch.object(agent_service.planner, "plan", side_effect=_deterministic_plan):
        r = client.post(
            "/api/v1/agent/run",
            json=payload,
            headers={"X-Tenant-Id": org, "X-User-Role": "member"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["response_type"] in {"query_result", "blocked"}


def test_agent_run_real_execution_path_enabled() -> None:
    org = "org_agent_real_path"
    payload = {
        "org_id": org,
        "user_id": "user_1",
        "message": "show me dau",
        "session_id": "sess_real_1",
    }
    original = agent_service.settings.allow_real_query_execution
    agent_service.settings.allow_real_query_execution = True
    try:
        with patch.object(agent_service.planner, "plan", side_effect=_deterministic_plan):
            r = client.post(
                "/api/v1/agent/run",
                json=payload,
                headers={"X-Tenant-Id": org, "X-User-Role": "member"},
            )
    finally:
        agent_service.settings.allow_real_query_execution = original

    assert r.status_code == 200
    body = r.json()
    assert body["response_type"] in {"query_result", "blocked", "partial_failure"}
