from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.db.session import get_db
from data_autopilot.models.entities import Role
from data_autopilot.schemas.common import (
    AgentRequest,
    AgentResponse,
    FeedbackRequest,
    FeedbackResponse,
    HealthResponse,
)
from data_autopilot.security.rbac import require_member_or_admin, role_from_headers
from data_autopilot.security.tenancy import ensure_tenant_scope, tenant_from_headers
from data_autopilot.api.state import (
    agent_service,
    audit_service,
    bigquery_connector,
    feedback_service,
    metabase_client,
)


router = APIRouter()


@router.get("/app", response_class=HTMLResponse)
def app_shell() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Data Team Autopilot Chat</title>
  <style>
    body { font-family: ui-sans-serif, -apple-system, Segoe UI, sans-serif; margin: 0; background: #f4f6fb; color: #111827; }
    .wrap { max-width: 900px; margin: 40px auto; padding: 0 16px; }
    .card { background: #fff; border-radius: 12px; padding: 18px; box-shadow: 0 8px 20px rgba(17,24,39,.08); }
    .meta { display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 10px; margin-bottom: 14px; }
    input, textarea, button { font: inherit; }
    input, textarea { width: 100%; border: 1px solid #d1d5db; border-radius: 8px; padding: 10px; box-sizing: border-box; }
    button { border: 0; border-radius: 8px; background: #0f172a; color: #fff; padding: 10px 14px; cursor: pointer; }
    .hint { color: #4b5563; font-size: 14px; margin-top: 8px; }
    #modal { position: fixed; inset: 0; display: none; align-items: center; justify-content: center; background: rgba(17,24,39,.45); }
    #modal.open { display: flex; }
    #panel { width: min(760px, 92vw); background: #fff; border-radius: 12px; padding: 16px; }
    #output { white-space: pre-wrap; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; min-height: 120px; max-height: 360px; overflow: auto; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h2>In-App Data Bot</h2>
      <div class="hint">Open chat with <strong>Cmd/Ctrl + K</strong>. Enter sends, Shift+Enter adds newline, Esc closes.</div>
    </div>
  </div>
  <div id="modal">
    <div id="panel">
      <div class="meta">
        <input id="org" placeholder="Org ID" />
        <input id="user" placeholder="User ID" />
        <input id="role" placeholder="Role (member/admin)" value="member" />
      </div>
      <textarea id="prompt" rows="4" placeholder="Ask a question..."></textarea>
      <div style="display:flex; gap:10px; margin-top:10px;">
        <button id="send">Send</button>
        <button id="close" type="button">Close</button>
      </div>
      <div id="output" style="margin-top:12px;"></div>
    </div>
  </div>
  <script>
    const modal = document.getElementById('modal');
    const promptEl = document.getElementById('prompt');
    const output = document.getElementById('output');
    const org = document.getElementById('org');
    const user = document.getElementById('user');
    const role = document.getElementById('role');

    function openModal() { modal.classList.add('open'); promptEl.focus(); }
    function closeModal() { modal.classList.remove('open'); }
    function persist() {
      localStorage.setItem('dta_org', org.value.trim());
      localStorage.setItem('dta_user', user.value.trim());
      localStorage.setItem('dta_role', role.value.trim() || 'member');
    }
    org.value = localStorage.getItem('dta_org') || 'org_demo';
    user.value = localStorage.getItem('dta_user') || 'user_demo';
    role.value = localStorage.getItem('dta_role') || 'member';

    async function sendPrompt() {
      persist();
      const message = promptEl.value.trim();
      if (!message) return;
      output.textContent = 'Working...';
      const payload = {
        org_id: org.value.trim(),
        user_id: user.value.trim(),
        message,
        session_id: 'web-' + Date.now()
      };
      const res = await fetch('/api/v1/agent/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Tenant-Id': payload.org_id,
          'X-User-Role': role.value.trim() || 'member'
        },
        body: JSON.stringify(payload)
      });
      const json = await res.json();
      output.textContent = JSON.stringify(json, null, 2);
      promptEl.value = '';
    }

    document.getElementById('send').addEventListener('click', sendPrompt);
    document.getElementById('close').addEventListener('click', closeModal);
    document.addEventListener('keydown', (e) => {
      const hotkey = (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k';
      if (hotkey) { e.preventDefault(); openModal(); }
      if (e.key === 'Escape') closeModal();
      if (modal.classList.contains('open') && e.key === 'Enter' && !e.shiftKey && e.target === promptEl) {
        e.preventDefault();
        sendPrompt();
      }
    });
  </script>
</body>
</html>"""


@router.get('/health', response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", app=settings.app_name)


@router.get('/ready')
def ready() -> dict:
    settings = get_settings()
    checks: dict[str, dict] = {}

    checks["bigquery"] = {"ok": True, "mode": "mock"} if settings.bigquery_mock_mode else bigquery_connector.test_connection()
    checks["metabase"] = {"ok": True, "mode": "mock"} if settings.metabase_mock_mode else metabase_client.test_connection()

    ok = all(bool(v.get("ok")) for v in checks.values())
    return {"ok": ok, "checks": checks}


@router.post('/api/v1/agent/run', response_model=AgentResponse)
def run_agent(
    req: AgentRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> AgentResponse:
    ensure_tenant_scope(tenant_id, req.org_id)
    require_member_or_admin(role)

    result = agent_service.run(db=db, org_id=req.org_id, user_id=req.user_id, message=req.message)
    audit_service.log(
        db,
        tenant_id=req.org_id,
        event_type="agent_run",
        payload={"user_id": req.user_id, "session_id": req.session_id, "response_type": result.get("response_type")},
    )
    return AgentResponse(**result)


@router.post('/api/v1/feedback', response_model=FeedbackResponse)
def create_feedback(
    req: FeedbackRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> FeedbackResponse:
    ensure_tenant_scope(tenant_id, req.tenant_id)
    row = feedback_service.create(db, req)
    audit_service.log(
        db,
        tenant_id=req.tenant_id,
        event_type="feedback_created",
        payload={
            "artifact_id": req.artifact_id,
            "artifact_version": req.artifact_version,
            "artifact_type": req.artifact_type,
            "feedback_type": req.feedback_type,
        },
    )
    return FeedbackResponse(id=row.id, created_at=row.created_at)


@router.get('/api/v1/feedback/summary')
def feedback_summary(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    summary = feedback_service.summary(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="feedback_summary_viewed", payload={"org_id": org_id})
    return summary
