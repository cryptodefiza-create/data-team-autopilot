from fastapi.testclient import TestClient

from data_autopilot.api.state import agent_service
from data_autopilot.main import app


client = TestClient(app)


def test_agent_blocked_response_contains_approval_actions() -> None:
    org = "org_agent_approval_contract"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    old_soft = agent_service.critic.settings.per_query_max_bytes
    old_hard = agent_service.critic.settings.per_query_max_bytes_with_approval
    try:
        agent_service.critic.settings.per_query_max_bytes = 1
        agent_service.critic.settings.per_query_max_bytes_with_approval = 1_000_000
        resp = client.post(
            "/api/v1/agent/run",
            headers=headers,
            json={"org_id": org, "user_id": "u1", "session_id": "s1", "message": "show me dau"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["response_type"] == "blocked"
        assert body["data"]["gate"]["approval_required"] is True
        assert body["data"]["approval"]["endpoint_preview"] == "/api/v1/queries/preview"
        assert body["data"]["approval"]["endpoint_approve_run"] == "/api/v1/queries/approve-run"
    finally:
        agent_service.critic.settings.per_query_max_bytes = old_soft
        agent_service.critic.settings.per_query_max_bytes_with_approval = old_hard
