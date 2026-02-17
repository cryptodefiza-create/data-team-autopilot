from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.db.session import get_db
from data_autopilot.models.entities import Role
from data_autopilot.schemas.common import (
    AgentRequest,
    AgentResponse,
    ChatRequest,
    ChatResponse,
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
    conversation_service,
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


@router.get("/tester", response_class=HTMLResponse)
def tester_shell() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Data Team Autopilot Tester</title>
  <style>
    :root { color-scheme: light; }
    body { margin: 0; font-family: ui-sans-serif, -apple-system, Segoe UI, sans-serif; background: #f6f8fc; color: #0f172a; }
    .wrap { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
    .grid { display: grid; gap: 14px; grid-template-columns: 1fr 1fr; }
    .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 14px; box-shadow: 0 4px 14px rgba(15, 23, 42, .05); }
    .full { grid-column: 1 / -1; }
    h1 { margin: 0 0 8px; font-size: 24px; }
    h2 { margin: 0 0 8px; font-size: 16px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; }
    input, textarea, button, select { font: inherit; }
    input, textarea, select { width: 100%; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px; box-sizing: border-box; background: #fff; }
    button { border: 0; border-radius: 8px; background: #0f172a; color: #fff; padding: 9px 12px; cursor: pointer; }
    button.alt { background: #334155; }
    .k { font-weight: 600; }
    pre { margin: 0; white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px; max-height: 280px; overflow: auto; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; border-bottom: 1px solid #e5e7eb; padding: 8px 6px; font-size: 13px; vertical-align: top; }
    .ok { color: #166534; font-weight: 700; }
    .bad { color: #991b1b; font-weight: 700; }
    @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Data Team Autopilot Tester Console</h1>
    <div class="card full">
      <div class="row">
        <div style="flex:1;min-width:220px;">
          <div class="k">Org ID</div>
          <input id="org" value="org_alpha" />
        </div>
        <div style="flex:1;min-width:220px;">
          <div class="k">User ID</div>
          <input id="user" value="tester_1" />
        </div>
        <div style="width:180px;">
          <div class="k">Role</div>
          <select id="role">
            <option value="member">member</option>
            <option value="admin">admin</option>
            <option value="viewer">viewer</option>
          </select>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>Environment Status</h2>
        <div id="status"></div>
        <div class="row" style="margin-top:8px;">
          <button id="refreshStatus">Refresh Status</button>
        </div>
      </div>

      <div class="card">
        <h2>Run Workflows</h2>
        <div class="row">
          <button data-wf="profile">Run Profile</button>
          <button data-wf="dashboard">Run Dashboard</button>
          <button data-wf="memo">Run Memo</button>
        </div>
        <div style="margin-top:8px;">
          <pre id="wfOut"></pre>
        </div>
      </div>

      <div class="card full">
        <h2>Chat Test (Intent Orchestration)</h2>
        <textarea id="prompt" rows="3" placeholder="Ask: show me dau last 14 days"></textarea>
        <div class="row" style="margin-top:8px;">
          <button id="runChat">Send Prompt</button>
        </div>
        <div style="margin-top:8px;">
          <pre id="chatOut"></pre>
        </div>
      </div>

      <div class="card full">
        <h2>Artifacts</h2>
        <div class="row" style="margin-bottom:8px;">
          <button id="refreshArtifacts" class="alt">Refresh Artifacts</button>
        </div>
        <table>
          <thead><tr><th>ID</th><th>Type</th><th>Version</th><th>Created</th><th>Action</th></tr></thead>
          <tbody id="artifactRows"></tbody>
        </table>
        <div style="margin-top:8px;">
          <pre id="artifactOut"></pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    const statusEl = document.getElementById("status");
    const wfOut = document.getElementById("wfOut");
    const chatOut = document.getElementById("chatOut");
    const artifactRows = document.getElementById("artifactRows");
    const artifactOut = document.getElementById("artifactOut");
    const orgEl = document.getElementById("org");
    const userEl = document.getElementById("user");
    const roleEl = document.getElementById("role");

    function headers() {
      const org = orgEl.value.trim();
      const role = roleEl.value.trim() || "member";
      localStorage.setItem("dta_org", org);
      localStorage.setItem("dta_user", userEl.value.trim());
      localStorage.setItem("dta_role", role);
      return {
        "Content-Type": "application/json",
        "X-Tenant-Id": org,
        "X-User-Role": role
      };
    }

    async function refreshStatus() {
      const [readyRes, llmRes] = await Promise.all([
        fetch("/ready"),
        fetch("/api/v1/llm/status")
      ]);
      const ready = await readyRes.json();
      const llm = await llmRes.json();
      const modeClass = llm.mode === "llm" ? "ok" : "bad";
      statusEl.innerHTML = `
        <div><span class="k">Readiness:</span> ${ready.ok ? '<span class="ok">OK</span>' : '<span class="bad">NOT READY</span>'}</div>
        <div><span class="k">BigQuery:</span> ${ready.checks.bigquery.mode}</div>
        <div><span class="k">Metabase:</span> ${ready.checks.metabase.mode}</div>
        <div><span class="k">Planner/Memo mode:</span> <span class="${modeClass}">${llm.mode}</span></div>
        <div><span class="k">LLM model:</span> ${llm.model || "(not configured)"}</div>
        <div><span class="k">LLM base URL:</span> ${llm.base_url}</div>
      `;
    }

    async function runWorkflow(kind) {
      const org = encodeURIComponent(orgEl.value.trim());
      const res = await fetch(`/api/v1/workflows/${kind}?org_id=${org}`, {
        method: "POST",
        headers: headers()
      });
      const body = await res.json();
      wfOut.textContent = JSON.stringify(body, null, 2);
      await refreshArtifacts();
    }

    async function runChat() {
      const body = {
        org_id: orgEl.value.trim(),
        user_id: userEl.value.trim() || "tester_1",
        session_id: "tester-" + Date.now(),
        message: document.getElementById("prompt").value.trim()
      };
      const res = await fetch("/api/v1/chat/run", {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(body)
      });
      const json = await res.json();
      chatOut.textContent = JSON.stringify(json, null, 2);
    }

    async function refreshArtifacts() {
      const org = encodeURIComponent(orgEl.value.trim());
      const res = await fetch(`/api/v1/artifacts?org_id=${org}`, { headers: headers() });
      const data = await res.json();
      artifactRows.innerHTML = "";
      (data.items || []).forEach((item) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${item.artifact_id}</td>
          <td>${item.type}</td>
          <td>${item.version}</td>
          <td>${item.created_at}</td>
          <td><button class="alt" data-id="${item.artifact_id}">View</button></td>
        `;
        artifactRows.appendChild(tr);
      });
    }

    async function viewArtifact(id) {
      const org = encodeURIComponent(orgEl.value.trim());
      const res = await fetch(`/api/v1/artifacts/${encodeURIComponent(id)}?org_id=${org}`, { headers: headers() });
      const data = await res.json();
      artifactOut.textContent = JSON.stringify(data, null, 2);
      if (data.type === "dashboard" && data.data && data.data.metabase_dashboard_id) {
        const dashId = data.data.metabase_dashboard_id;
        const hint = "\\n\\nMetabase dashboard: " + dashId;
        artifactOut.textContent += hint;
      }
    }

    document.getElementById("refreshStatus").addEventListener("click", refreshStatus);
    document.getElementById("runChat").addEventListener("click", runChat);
    document.getElementById("refreshArtifacts").addEventListener("click", refreshArtifacts);
    document.querySelectorAll("button[data-wf]").forEach((el) => {
      el.addEventListener("click", () => runWorkflow(el.getAttribute("data-wf")));
    });
    artifactRows.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-id]");
      if (btn) viewArtifact(btn.getAttribute("data-id"));
    });

    orgEl.value = localStorage.getItem("dta_org") || orgEl.value;
    userEl.value = localStorage.getItem("dta_user") || userEl.value;
    roleEl.value = localStorage.getItem("dta_role") || roleEl.value;
    refreshStatus();
    refreshArtifacts();
  </script>
</body>
</html>"""


@router.get("/chat", response_class=HTMLResponse)
def chat_shell() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Data Team Autopilot Chat</title>
  <style>
    body { margin: 0; font-family: ui-sans-serif, -apple-system, Segoe UI, sans-serif; background: #f3f6fc; color: #0f172a; }
    .app { max-width: 1100px; margin: 20px auto; padding: 0 16px; }
    .top { display: grid; grid-template-columns: 1fr 1fr 160px auto; gap: 10px; margin-bottom: 12px; }
    .panel { background: #fff; border: 1px solid #dbe3ef; border-radius: 12px; box-shadow: 0 6px 20px rgba(15, 23, 42, .06); }
    .chat { height: 62vh; overflow: auto; padding: 14px; display: flex; flex-direction: column; gap: 10px; }
    .msg { border-radius: 10px; padding: 10px 12px; max-width: 82%; white-space: pre-wrap; word-break: break-word; }
    .user { align-self: flex-end; background: #0f172a; color: #fff; }
    .bot { align-self: flex-start; background: #eef2ff; color: #111827; border: 1px solid #dbeafe; }
    .meta { font-size: 12px; color: #64748b; margin-top: 6px; }
    .composer { display: grid; grid-template-columns: 1fr auto; gap: 10px; padding: 12px; border-top: 1px solid #e5e7eb; }
    textarea, input, select, button { font: inherit; }
    textarea, input, select { width: 100%; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px; box-sizing: border-box; }
    button { border: 0; border-radius: 8px; background: #0f172a; color: #fff; padding: 10px 14px; cursor: pointer; }
    .hint { margin: 10px 0; color: #475569; font-size: 13px; }
    .chips { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
    .chip { background: #e2e8f0; color: #0f172a; border: 0; border-radius: 999px; padding: 7px 10px; cursor: pointer; }
    .status { margin-bottom: 10px; font-size: 13px; color: #334155; }
    @media (max-width: 900px) {
      .top { grid-template-columns: 1fr; }
      .msg { max-width: 95%; }
    }
  </style>
</head>
<body>
  <div class="app">
    <div class="top">
      <input id="org" value="org_alpha" placeholder="Org ID">
      <input id="user" value="tester_1" placeholder="User ID">
      <select id="role">
        <option value="member">member</option>
        <option value="admin">admin</option>
        <option value="viewer">viewer</option>
      </select>
      <button id="refresh">Refresh Status</button>
    </div>
    <div class="status" id="status">Loading status...</div>
    <div class="panel">
      <div class="chat" id="chat"></div>
      <div class="composer">
        <div>
          <div class="chips">
            <button class="chip" data-msg="Profile my warehouse and refresh the catalog">Profile Data</button>
            <button class="chip" data-msg="Create an executive dashboard from available tables">Create Dashboard</button>
            <button class="chip" data-msg="Generate my weekly executive memo">Generate Memo</button>
            <button class="chip" data-msg="Show me DAU for the last 14 days">Run Data Pull</button>
          </div>
          <textarea id="prompt" rows="3" placeholder="Describe what you need. Example: build a KPI dashboard for revenue and active users."></textarea>
          <div class="hint">This chat executes real workflows/queries through safety and budget gates.</div>
        </div>
        <button id="send">Send</button>
      </div>
    </div>
  </div>
  <script>
    const chat = document.getElementById("chat");
    const promptEl = document.getElementById("prompt");
    const orgEl = document.getElementById("org");
    const userEl = document.getElementById("user");
    const roleEl = document.getElementById("role");
    const statusEl = document.getElementById("status");

    function headers() {
      const org = orgEl.value.trim();
      const role = roleEl.value.trim() || "member";
      localStorage.setItem("dta_org", org);
      localStorage.setItem("dta_user", userEl.value.trim());
      localStorage.setItem("dta_role", role);
      return {
        "Content-Type": "application/json",
        "X-Tenant-Id": org,
        "X-User-Role": role
      };
    }

    function appendMsg(kind, text, meta = "") {
      const box = document.createElement("div");
      box.className = "msg " + (kind === "user" ? "user" : "bot");
      box.textContent = text;
      chat.appendChild(box);
      if (meta) {
        const m = document.createElement("div");
        m.className = "meta";
        m.textContent = meta;
        chat.appendChild(m);
      }
      chat.scrollTop = chat.scrollHeight;
    }

    async function refreshStatus() {
      const [readyRes, llmRes] = await Promise.all([
        fetch("/ready"),
        fetch("/api/v1/llm/status")
      ]);
      const ready = await readyRes.json();
      const llm = await llmRes.json();
      statusEl.textContent =
        `Ready: ${ready.ok ? "yes" : "no"} | BigQuery: ${ready.checks.bigquery.mode} | Metabase: ${ready.checks.metabase.mode} | LLM mode: ${llm.mode}${llm.model ? ` (${llm.model})` : ""}`;
    }

    function formatAssistantResponse(resp) {
      const summary = resp.summary || "Completed.";
      const details = resp.data ? JSON.stringify(resp.data, null, 2) : "{}";
      return summary + "\\n\\n" + details;
    }

    async function sendPrompt() {
      const message = promptEl.value.trim();
      if (!message) return;
      appendMsg("user", message);
      promptEl.value = "";
      appendMsg("bot", "Working...");
      const loadingIndex = chat.children.length - 1;
      try {
        const payload = {
          org_id: orgEl.value.trim(),
          user_id: userEl.value.trim() || "tester_1",
          session_id: "chat-" + Date.now(),
          message
        };
        const res = await fetch("/api/v1/chat/run", {
          method: "POST",
          headers: headers(),
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        chat.children[loadingIndex].textContent = formatAssistantResponse(data);
      } catch (err) {
        chat.children[loadingIndex].textContent = "Request failed: " + String(err);
      }
    }

    document.querySelectorAll(".chip").forEach((el) => {
      el.addEventListener("click", () => {
        promptEl.value = el.getAttribute("data-msg") || "";
        promptEl.focus();
      });
    });
    document.getElementById("refresh").addEventListener("click", refreshStatus);
    document.getElementById("send").addEventListener("click", sendPrompt);
    promptEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendPrompt();
      }
    });

    orgEl.value = localStorage.getItem("dta_org") || orgEl.value;
    userEl.value = localStorage.getItem("dta_user") || userEl.value;
    roleEl.value = localStorage.getItem("dta_role") || roleEl.value;
    refreshStatus();
    appendMsg("bot", "Describe what you need: dashboards, memo, profiling, or data pulls.");
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


@router.get("/api/v1/llm/status")
def llm_status() -> dict:
    settings = get_settings()
    configured = bool(settings.llm_api_key and settings.llm_model)
    from data_autopilot.services.llm_client import get_eval_providers

    eval_providers = get_eval_providers()
    return {
        "mode": "llm" if configured else "fallback",
        "configured": configured,
        "model": settings.llm_model or None,
        "base_url": settings.llm_api_base_url,
        "eval_enabled": settings.llm_eval_enabled,
        "eval_providers": [
            {"name": p.name, "model": p.model} for p in eval_providers
        ],
    }


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


@router.post('/api/v1/chat/run', response_model=ChatResponse)
def run_chat(
    req: ChatRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> ChatResponse:
    ensure_tenant_scope(tenant_id, req.org_id)
    require_member_or_admin(role)

    result = conversation_service.respond(
        db=db,
        tenant_id=req.org_id,
        user_id=req.user_id,
        message=req.message,
    )
    audit_service.log(
        db,
        tenant_id=req.org_id,
        event_type="chat_run",
        payload={
            "user_id": req.user_id,
            "session_id": req.session_id,
            "response_type": result.get("response_type"),
            "intent_action": (result.get("meta") or {}).get("intent_action"),
        },
    )
    return ChatResponse(**result)


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


@router.get('/api/v1/llm/eval-runs')
def list_eval_runs(
    org_id: str,
    task_type: str | None = None,
    limit: int = 50,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """List recent LLM evaluation runs for analysis and comparison."""
    from data_autopilot.models.entities import AuditLog
    from sqlalchemy import select

    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)

    stmt = (
        select(AuditLog)
        .where(AuditLog.tenant_id == org_id, AuditLog.event_type == "llm_eval_run")
        .order_by(AuditLog.created_at.desc())
        .limit(min(limit, 200))
    )
    rows = db.execute(stmt).scalars().all()

    items = []
    for row in rows:
        payload = row.payload or {}
        if task_type and payload.get("task_type") != task_type:
            continue
        items.append({
            "run_id": payload.get("run_id"),
            "task_type": payload.get("task_type"),
            "started_at": payload.get("started_at"),
            "primary": payload.get("primary"),
            "evaluations": payload.get("evaluations", []),
        })

    return {"org_id": org_id, "count": len(items), "items": items}
