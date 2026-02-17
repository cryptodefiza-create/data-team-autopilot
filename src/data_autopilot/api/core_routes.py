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

    let wfBusy = false;
    async function runWorkflow(kind) {
      if (wfBusy) return;
      wfBusy = true;
      document.querySelectorAll("[data-wf]").forEach(b => { b.disabled = true; });
      wfOut.textContent = "Running " + kind + "...";
      try {
        const org = encodeURIComponent(orgEl.value.trim());
        const res = await fetch(`/api/v1/workflows/${kind}?org_id=${org}`, {
          method: "POST",
          headers: headers()
        });
        const body = await res.json();
        wfOut.textContent = JSON.stringify(body, null, 2);
        await refreshArtifacts();
      } catch (err) {
        wfOut.textContent = "Error: " + String(err);
      } finally {
        wfBusy = false;
        document.querySelectorAll("[data-wf]").forEach(b => { b.disabled = false; });
      }
    }

    let chatBusy = false;
    async function runChat() {
      if (chatBusy) return;
      chatBusy = true;
      const chatBtn = document.getElementById("runChat");
      chatBtn.disabled = true;
      chatBtn.textContent = "Sending...";
      chatOut.textContent = "Working...";
      try {
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
      } catch (err) {
        chatOut.textContent = "Error: " + String(err);
      } finally {
        chatBusy = false;
        chatBtn.disabled = false;
        chatBtn.textContent = "Send Prompt";
      }
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

    let sendingChat = false;
    async function sendPrompt() {
      const message = promptEl.value.trim();
      if (!message || sendingChat) return;
      sendingChat = true;
      const sendBtn = document.getElementById("send");
      sendBtn.disabled = true;
      sendBtn.textContent = "Sending...";
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
      } finally {
        sendingChat = false;
        sendBtn.disabled = false;
        sendBtn.textContent = "Send";
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


@router.get("/tester-app", response_class=HTMLResponse)
def tester_app_shell() -> str:
    return _TESTER_APP_HTML


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


@router.get('/api/v1/llm/usage')
def llm_usage(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """Per-org LLM token usage and cost summary by provider."""
    from data_autopilot.services.llm_cost_service import LLMCostService

    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    svc = LLMCostService()
    return svc.get_usage_summary(db, tenant_id=org_id)


@router.get('/api/v1/llm/budget')
def llm_budget(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """Current LLM budget status for an org."""
    from data_autopilot.services.llm_cost_service import LLMCostService
    from dataclasses import asdict

    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    svc = LLMCostService()
    status = svc.get_budget_status(db, tenant_id=org_id)
    return asdict(status)


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


@router.get('/api/v1/feedback/provider-summary')
def feedback_provider_summary(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    return feedback_service.provider_summary(db, tenant_id=org_id)


@router.get('/api/v1/feedback/review')
def feedback_review(
    org_id: str,
    status: str | None = None,
    provider: str | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    from data_autopilot.security.rbac import require_admin

    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    items = feedback_service.list_for_review(db, tenant_id=org_id, status=status, provider=provider)
    return {"org_id": org_id, "count": len(items), "items": items}


@router.post('/api/v1/feedback/{feedback_id}/resolve')
def resolve_feedback(
    feedback_id: str,
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    from fastapi import HTTPException
    from data_autopilot.security.rbac import require_admin

    require_admin(role)
    resolved_by = req.get("resolved_by", "admin")
    row = feedback_service.resolve(db, feedback_id=feedback_id, resolved_by=resolved_by)
    if not row:
        raise HTTPException(status_code=404, detail="Feedback not found")
    audit_service.log(
        db,
        tenant_id=tenant_id,
        event_type="feedback_resolved",
        payload={"feedback_id": feedback_id, "resolved_by": resolved_by},
    )
    return {"id": row.id, "resolved": True, "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None}


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


@router.post('/api/v1/llm/evaluate-memo')
def evaluate_memo_providers(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """Run memo generation across primary + eval providers and score validation.

    Accepts optional `packet` (uses demo packet if omitted) and
    `runs_per_provider` (default 1, max 50).
    """
    from data_autopilot.security.rbac import require_admin
    from data_autopilot.services.memo_service import (
        MemoService,
        validate_causes,
        validate_coverage,
        validate_metric_names,
        validate_numbers,
    )
    from data_autopilot.services.llm_client import LLMClient, get_eval_providers

    org_id = str(req.get("org_id", tenant_id))
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)

    runs_per_provider = min(int(req.get("runs_per_provider", 1)), 50)

    packet = req.get("packet")
    if not packet:
        packet = _demo_memo_packet()

    svc = MemoService()
    primary_client = LLMClient()
    eval_providers = get_eval_providers()

    # Build list of (name, LLMClient) pairs to evaluate
    clients: list[tuple[str, LLMClient]] = []
    if primary_client.is_configured():
        p = primary_client.provider
        clients.append((p.name if p else "primary", primary_client))
    for ep in eval_providers:
        clients.append((ep.name, LLMClient(provider=ep)))

    if not clients:
        return {
            "org_id": org_id,
            "error": "No LLM providers configured. Set LLM_API_KEY or enable eval providers.",
            "results": {},
        }

    import json as _json
    import time

    results: dict[str, dict] = {}
    for provider_name, client in clients:
        stats = {
            "runs": runs_per_provider,
            "valid_json": 0,
            "passed_number_check": 0,
            "passed_metric_check": 0,
            "passed_coverage_check": 0,
            "passed_cause_check": 0,
            "passed_all_checks": 0,
            "total_latency_ms": 0.0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "errors": [],
            "samples": [],
        }

        system_prompt = svc._build_system_prompt()
        user_prompt = "Create weekly memo from this packet:\n" + _json.dumps(packet, sort_keys=True)

        for i in range(runs_per_provider):
            t0 = time.perf_counter()
            result = client.generate_json_with_meta(system_prompt, user_prompt)
            elapsed = (time.perf_counter() - t0) * 1000
            stats["total_latency_ms"] += elapsed
            stats["total_input_tokens"] += result.input_tokens
            stats["total_output_tokens"] += result.output_tokens

            if not result.succeeded:
                stats["errors"].append({"run": i + 1, "error": result.error})
                continue

            memo = result.content
            # Check structure
            is_valid = all(
                isinstance(memo.get(k), list)
                for k in ("headline_summary", "key_changes", "likely_causes", "recommended_actions", "data_quality_notes")
            )
            if is_valid:
                stats["valid_json"] += 1
            else:
                stats["errors"].append({"run": i + 1, "error": "Invalid JSON structure"})
                continue

            num_errs = validate_numbers(memo, packet)
            metric_errs = validate_metric_names(memo, packet)
            cov_warns = validate_coverage(memo, packet)
            cause_errs = validate_causes(memo, packet)

            if not num_errs:
                stats["passed_number_check"] += 1
            if not metric_errs:
                stats["passed_metric_check"] += 1
            if not cov_warns:
                stats["passed_coverage_check"] += 1
            if not cause_errs:
                stats["passed_cause_check"] += 1
            if not any([num_errs, metric_errs, cov_warns, cause_errs]):
                stats["passed_all_checks"] += 1

            if len(stats["samples"]) < 3:
                stats["samples"].append({
                    "run": i + 1,
                    "number_errors": num_errs,
                    "metric_errors": metric_errs,
                    "coverage_warnings": cov_warns,
                    "cause_errors": cause_errs,
                    "latency_ms": round(elapsed, 2),
                    "memo_keys": list(memo.keys()),
                })

        stats["avg_latency_ms"] = round(stats["total_latency_ms"] / runs_per_provider, 2)
        results[provider_name] = stats

    # Blind labeling: deterministic per-org shuffle
    import random as _random

    provider_names = sorted(results.keys())
    rng = _random.Random(org_id)
    rng.shuffle(provider_names)
    blind_mapping: dict[str, str] = {}
    blind_results: dict[str, dict] = {}
    for idx, real_name in enumerate(provider_names):
        label = f"Model {chr(65 + idx)}"
        blind_mapping[label] = real_name
        blind_results[label] = results[real_name]

    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="memo_provider_evaluation",
        payload={
            "runs_per_provider": runs_per_provider,
            "results": results,
            "blind_mapping": blind_mapping,
        },
    )

    return {
        "org_id": org_id,
        "runs_per_provider": runs_per_provider,
        "blind_mode": True,
        "results": blind_results,
    }


_TESTER_APP_HTML = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Data Team Autopilot — Tester</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          fontFamily: { sans: ['DM Sans', 'ui-sans-serif', 'system-ui'], mono: ['JetBrains Mono', 'monospace'] },
          colors: { accent: { DEFAULT: '#10b981', light: '#34d399', dim: '#065f46' } }
        }
      }
    }
  </script>
  <style>
    [x-cloak] { display: none !important; }
    .scrollbar-thin::-webkit-scrollbar { width: 6px; }
    .scrollbar-thin::-webkit-scrollbar-track { background: transparent; }
    .scrollbar-thin::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    .provider-grok { color: #60a5fa; }
    .provider-gpt { color: #4ade80; }
    .provider-claude { color: #fb923c; }
    @keyframes pulse-dot { 0%,100% { opacity:.4; } 50% { opacity:1; } }
    .loading-dot { animation: pulse-dot 1.2s ease-in-out infinite; }
  </style>
</head>
<body class="dark bg-slate-950 text-slate-200 font-sans min-h-screen">
<div x-data="testerApp()" x-init="init()" x-cloak class="flex flex-col h-screen">

  <!-- ===== HEADER / STATUS BAR ===== -->
  <header class="flex items-center justify-between px-4 py-2 bg-slate-900 border-b border-slate-800 shrink-0">
    <div class="flex items-center gap-3">
      <h1 class="text-base font-bold text-white tracking-tight">Data Team Autopilot</h1>
      <span class="text-xs bg-accent/20 text-accent px-2 py-0.5 rounded-full font-medium">Tester Preview</span>
      <span x-show="feedbackCount > 0" class="text-xs bg-slate-800 text-slate-300 px-2 py-0.5 rounded-full" x-text="feedbackCount + ' feedback'"></span>
    </div>
    <div class="flex items-center gap-4 text-xs">
      <label class="flex items-center gap-1.5">
        <span class="text-slate-400">Org</span>
        <input x-model="orgId" @change="persistOrg()" class="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-xs w-28 text-slate-200 focus:outline-none focus:ring-1 focus:ring-accent" />
      </label>
      <div class="flex items-center gap-1.5">
        <span class="w-2 h-2 rounded-full" :class="status.llm_ok ? 'bg-accent' : 'bg-red-500'"></span>
        <span class="text-slate-400">LLM:</span>
        <span class="text-slate-200" x-text="status.llm_model || 'n/a'"></span>
      </div>
      <div class="flex items-center gap-1.5">
        <span class="w-2 h-2 rounded-full" :class="status.bq_ok ? 'bg-accent' : 'bg-red-500'"></span>
        <span class="text-slate-400">BQ:</span>
        <span class="text-slate-200" x-text="status.bq_mode"></span>
      </div>
      <div class="flex items-center gap-1.5">
        <span class="w-2 h-2 rounded-full" :class="status.mb_ok ? 'bg-accent' : 'bg-red-500'"></span>
        <span class="text-slate-400">Metabase:</span>
        <span class="text-slate-200" x-text="status.mb_mode"></span>
      </div>
    </div>
  </header>

  <!-- ===== MAIN TWO-PANEL AREA ===== -->
  <div class="flex flex-1 overflow-hidden">

    <!-- LEFT: Chat Panel -->
    <div class="flex flex-col w-1/2 border-r border-slate-800 lg:w-[45%]">

      <!-- Guided flow buttons -->
      <div class="flex flex-wrap gap-2 px-4 pt-3 pb-2 border-b border-slate-800">
        <button @click="sendGuided('Profile my warehouse and refresh the catalog')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition">Profile Warehouse</button>
        <button @click="sendGuided('Create an executive dashboard from available tables')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition">Build Dashboard</button>
        <button @click="sendGuided('Generate my weekly executive memo')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition">Generate Memo</button>
        <button @click="sendGuided('Show me DAU for the last 14 days')" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition">Free Chat</button>
      </div>

      <!-- Messages -->
      <div id="chatMessages" class="flex-1 overflow-y-auto px-4 py-3 space-y-4 scrollbar-thin">
        <template x-for="(msg, idx) in messages" :key="idx">
          <div>
            <!-- User message -->
            <div x-show="msg.role === 'user'" class="flex justify-end">
              <div class="bg-accent/20 text-accent-light rounded-xl rounded-br-sm px-4 py-2.5 max-w-[85%] text-sm whitespace-pre-wrap" x-text="msg.text"></div>
            </div>
            <!-- Assistant message -->
            <div x-show="msg.role === 'assistant'" class="space-y-1">
              <div class="bg-slate-800/80 border border-slate-700/50 rounded-xl rounded-bl-sm px-4 py-2.5 max-w-[90%] text-sm">
                <div class="whitespace-pre-wrap" x-text="msg.text"></div>
                <div x-show="msg.data" class="mt-2">
                  <pre class="font-mono text-xs bg-slate-900 rounded-lg p-3 overflow-x-auto max-h-60 scrollbar-thin text-slate-300" x-text="msg.dataFormatted"></pre>
                </div>
              </div>
              <!-- Meta line -->
              <div x-show="msg.provider" class="flex items-center gap-3 text-[11px] text-slate-500 pl-1">
                <span :class="providerColor(msg.provider)" x-text="msg.provider"></span>
                <span x-text="msg.latency ? msg.latency + 'ms' : ''"></span>
                <span x-text="msg.cost ? '$' + msg.cost : ''"></span>
                <button @click="compareModel(msg)" class="text-slate-500 hover:text-accent transition text-[11px] underline">Compare Models</button>
              </div>
              <!-- Feedback -->
              <div class="flex items-center gap-2 pl-1 mt-1">
                <button @click="submitFeedback(msg, 'positive')" class="text-slate-600 hover:text-accent transition" :class="msg.feedbackType === 'positive' && 'text-accent'">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M14 9V5a3 3 0 00-6 0v4H5a2 2 0 00-2 2v1.2l1.9 7.6A2 2 0 006.8 22h6.4a2 2 0 001.9-1.2L17 13.2V11a2 2 0 00-2-2h-1z"/></svg>
                </button>
                <button @click="msg.showFeedbackForm = !msg.showFeedbackForm; submitFeedback(msg, 'negative')" class="text-slate-600 hover:text-red-400 transition" :class="msg.feedbackType === 'negative' && 'text-red-400'">
                  <svg class="w-4 h-4 rotate-180" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M14 9V5a3 3 0 00-6 0v4H5a2 2 0 00-2 2v1.2l1.9 7.6A2 2 0 006.8 22h6.4a2 2 0 001.9-1.2L17 13.2V11a2 2 0 00-2-2h-1z"/></svg>
                </button>
                <div x-show="msg.showFeedbackForm" class="flex-1 max-w-xs">
                  <input x-model="msg.feedbackComment" @keydown.enter="submitFeedbackComment(msg)" placeholder="What was wrong?" class="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1 text-xs text-slate-300 focus:outline-none focus:ring-1 focus:ring-accent" />
                </div>
              </div>
            </div>
            <!-- Loading -->
            <div x-show="msg.role === 'loading'" class="flex gap-1.5 pl-1">
              <span class="w-2 h-2 bg-accent rounded-full loading-dot"></span>
              <span class="w-2 h-2 bg-accent rounded-full loading-dot" style="animation-delay:.2s"></span>
              <span class="w-2 h-2 bg-accent rounded-full loading-dot" style="animation-delay:.4s"></span>
            </div>
            <!-- Error -->
            <div x-show="msg.role === 'error'" class="bg-red-950/40 border border-red-900/50 rounded-xl px-4 py-2.5 max-w-[90%] text-sm text-red-300">
              <span x-text="msg.text"></span>
              <button @click="retryMessage(msg)" class="ml-2 text-red-400 hover:text-red-300 underline text-xs">Retry</button>
            </div>
          </div>
        </template>
      </div>

      <!-- Input bar -->
      <div class="px-4 py-3 border-t border-slate-800 bg-slate-900/50 shrink-0">
        <div class="flex gap-2">
          <textarea x-model="input" @keydown.enter.prevent="if (!$event.shiftKey) sendMessage()" rows="2" placeholder="Ask a question..." class="flex-1 bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 resize-none focus:outline-none focus:ring-1 focus:ring-accent placeholder-slate-500"></textarea>
          <button @click="sendMessage()" :disabled="sending" class="bg-accent hover:bg-accent-light disabled:opacity-40 text-white rounded-lg px-4 py-2 text-sm font-medium transition shrink-0">Send</button>
        </div>
      </div>
    </div>

    <!-- RIGHT: Context Panel -->
    <div class="flex flex-col w-1/2 lg:w-[55%]">

      <!-- Tabs -->
      <div class="flex gap-1 px-3 pt-2 pb-1 border-b border-slate-800 overflow-x-auto shrink-0">
        <template x-for="tab in contextTabs" :key="tab.id">
          <button @click="activeTab = tab.id" class="text-xs px-3 py-1.5 rounded-full whitespace-nowrap transition" :class="activeTab === tab.id ? 'bg-accent text-white' : 'bg-slate-800 text-slate-400 hover:text-slate-200'" x-text="tab.label"></button>
        </template>
      </div>

      <!-- Tab content -->
      <div class="flex-1 overflow-y-auto p-4 scrollbar-thin">

        <!-- Dashboards -->
        <div x-show="activeTab === 'dashboards'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Dashboards</h3>
          <div x-show="artifacts.dashboards.length === 0" class="text-xs text-slate-500">No dashboard artifacts found. Run "Build Dashboard" from chat.</div>
          <template x-for="a in artifacts.dashboards" :key="a.artifact_id">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2">
              <div class="flex justify-between items-center">
                <span class="font-mono text-xs text-slate-300" x-text="a.artifact_id"></span>
                <span class="text-[11px] text-slate-500" x-text="'v' + a.version"></span>
              </div>
              <div class="text-[11px] text-slate-500 mt-1" x-text="a.created_at"></div>
            </div>
          </template>
        </div>

        <!-- Memos -->
        <div x-show="activeTab === 'memos'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Memos</h3>
          <div x-show="artifacts.memos.length === 0" class="text-xs text-slate-500">No memo artifacts found. Run "Generate Memo" from chat.</div>
          <template x-for="a in artifacts.memos" :key="a.artifact_id">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2">
              <div class="flex justify-between items-center">
                <span class="font-mono text-xs text-slate-300" x-text="a.artifact_id"></span>
                <span class="text-[11px] text-slate-500" x-text="'v' + a.version"></span>
              </div>
              <div class="text-[11px] text-slate-500 mt-1" x-text="a.created_at"></div>
              <button @click="loadMemoDetail(a.artifact_id)" class="text-[11px] text-accent hover:text-accent-light mt-1 underline">View Detail</button>
            </div>
          </template>
          <div x-show="memoDetail">
            <pre class="font-mono text-xs bg-slate-900 rounded-lg p-3 mt-3 overflow-auto max-h-80 scrollbar-thin text-slate-300" x-text="memoDetail"></pre>
          </div>
        </div>

        <!-- Queries -->
        <div x-show="activeTab === 'queries'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Queries</h3>
          <div x-show="queries.length === 0" class="text-xs text-slate-500">Queries will appear here when chat returns query results.</div>
          <template x-for="(q, qi) in queries" :key="qi">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2">
              <pre class="font-mono text-xs text-slate-300 whitespace-pre-wrap" x-text="q.sql || 'N/A'"></pre>
              <div class="flex gap-3 mt-1 text-[11px] text-slate-500">
                <span x-show="q.bytes_scanned" x-text="'Bytes: ' + q.bytes_scanned"></span>
                <span x-show="q.cost" x-text="'Cost: $' + q.cost"></span>
              </div>
            </div>
          </template>
        </div>

        <!-- Profile -->
        <div x-show="activeTab === 'profile'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Warehouse Profile</h3>
          <div x-show="artifacts.profiles.length === 0" class="text-xs text-slate-500">No profile artifacts found. Run "Profile Warehouse" from chat.</div>
          <template x-for="a in artifacts.profiles" :key="a.artifact_id">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2">
              <div class="flex justify-between items-center">
                <span class="font-mono text-xs text-slate-300" x-text="a.artifact_id"></span>
                <span class="text-[11px] text-slate-500" x-text="'v' + a.version"></span>
              </div>
              <button @click="loadProfileDetail(a.artifact_id)" class="text-[11px] text-accent hover:text-accent-light mt-1 underline">View Catalog</button>
            </div>
          </template>
          <div x-show="profileDetail">
            <pre class="font-mono text-xs bg-slate-900 rounded-lg p-3 mt-3 overflow-auto max-h-80 scrollbar-thin text-slate-300" x-text="profileDetail"></pre>
          </div>
        </div>

        <!-- Audit -->
        <div x-show="activeTab === 'audit'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Audit Log</h3>
          <div x-show="auditEntries.length === 0" class="text-xs text-slate-500">Audit entries will appear from chat response metadata.</div>
          <template x-for="(entry, ei) in auditEntries" :key="ei">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2 text-xs">
              <div class="flex justify-between">
                <span class="text-slate-300 font-medium" x-text="entry.event_type"></span>
                <span class="text-slate-500" x-text="entry.timestamp"></span>
              </div>
              <pre class="font-mono text-[11px] text-slate-400 mt-1 whitespace-pre-wrap" x-text="entry.detail"></pre>
            </div>
          </template>
        </div>

        <!-- LLM Usage -->
        <div x-show="activeTab === 'llm_usage'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">LLM Usage &amp; Budget</h3>
          <button @click="loadLLMUsage()" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition mb-3">Refresh</button>
          <div x-show="llmUsage">
            <div class="grid grid-cols-2 gap-3 mb-3">
              <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3">
                <div class="text-[11px] text-slate-500">Total Requests</div>
                <div class="text-lg font-bold text-white" x-text="llmUsage?.total_requests ?? '-'"></div>
              </div>
              <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3">
                <div class="text-[11px] text-slate-500">Est. Cost</div>
                <div class="text-lg font-bold text-white" x-text="llmUsage?.total_estimated_cost_usd != null ? '$' + llmUsage.total_estimated_cost_usd.toFixed(4) : '-'"></div>
              </div>
            </div>
            <div x-show="llmUsage?.by_provider" class="space-y-2">
              <template x-for="(info, pname) in llmUsage?.by_provider || {}" :key="pname">
                <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 text-xs">
                  <div class="font-medium text-slate-300" :class="providerColor(pname)" x-text="pname"></div>
                  <div class="text-slate-500 mt-1">Model: <span class="text-slate-300" x-text="info.model"></span></div>
                  <div class="text-slate-500">Requests: <span class="text-slate-300" x-text="info.request_count"></span> | In: <span class="text-slate-300" x-text="info.input_tokens"></span> | Out: <span class="text-slate-300" x-text="info.output_tokens"></span></div>
                  <div class="text-slate-500">Cost: <span class="text-slate-300" x-text="'$' + info.estimated_cost_usd.toFixed(4)"></span></div>
                </div>
              </template>
            </div>
          </div>
          <div x-show="llmBudget" class="mt-4 bg-slate-800/60 border border-slate-700/40 rounded-lg p-3">
            <h4 class="text-xs font-semibold text-slate-300 mb-2">Budget Status</h4>
            <div class="flex items-center gap-3 text-xs">
              <span class="text-slate-500">Budget: <span class="text-slate-300" x-text="'$' + (llmBudget?.budget_usd ?? 0).toFixed(2)"></span></span>
              <span class="text-slate-500">Used: <span class="text-slate-300" x-text="(llmBudget?.usage_pct ?? 0).toFixed(1) + '%'"></span></span>
              <span class="text-slate-500">Remaining: <span class="text-slate-300" x-text="'$' + (llmBudget?.remaining_usd ?? 0).toFixed(4)"></span></span>
              <span x-show="llmBudget?.over_budget" class="text-red-400 font-medium">OVER BUDGET</span>
            </div>
          </div>
        </div>

        <!-- My Feedback -->
        <div x-show="activeTab === 'my_feedback'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">My Feedback</h3>
          <button @click="loadMyFeedback()" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-full transition mb-3">Refresh</button>
          <div x-show="myFeedbackItems.length === 0" class="text-xs text-slate-500">No feedback submitted yet.</div>
          <template x-for="(fb, fi) in myFeedbackItems" :key="fi">
            <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-3 mb-2 text-xs">
              <div class="flex justify-between items-center">
                <span class="font-medium" :class="fb.feedback_type === 'positive' ? 'text-accent' : 'text-red-400'" x-text="fb.feedback_type"></span>
                <span class="text-slate-500" x-text="fb.created_at"></span>
              </div>
              <div class="text-slate-400 mt-1" x-text="fb.artifact_type + ' — ' + fb.artifact_id"></div>
              <div x-show="fb.provider" class="text-slate-500 mt-0.5">Provider: <span class="text-slate-300" x-text="fb.provider"></span></div>
              <div x-show="fb.comment" class="text-slate-400 mt-1 italic" x-text="fb.comment"></div>
              <div class="mt-1">
                <span x-show="fb.resolved" class="text-accent text-[11px]">Resolved</span>
                <span x-show="!fb.resolved" class="text-slate-500 text-[11px]">Unresolved</span>
              </div>
            </div>
          </template>
        </div>

        <!-- Model Comparison -->
        <div x-show="activeTab === 'comparison'">
          <h3 class="text-sm font-semibold text-slate-300 mb-3">Model Comparison</h3>
          <button @click="runMemoEval()" :disabled="evalRunning" class="text-xs bg-accent hover:bg-accent-light disabled:opacity-40 text-white px-3 py-1.5 rounded-full transition mb-3">
            <span x-show="!evalRunning">Run Memo Evaluation</span>
            <span x-show="evalRunning">Running...</span>
          </button>
          <div x-show="!evalResults && !evalRunning" class="text-xs text-slate-500">Click "Run Memo Evaluation" or "Compare Models" on a chat response to see side-by-side provider results.</div>
          <div x-show="evalResults" class="grid grid-cols-1 gap-3">
            <template x-for="(stats, pname) in evalResults || {}" :key="pname">
              <div class="bg-slate-800/60 border border-slate-700/40 rounded-lg p-4">
                <div class="flex justify-between items-center mb-2">
                  <span class="font-semibold text-sm" :class="providerColor(pname)" x-text="pname"></span>
                  <span class="text-[11px] text-slate-500" x-text="'Avg ' + stats.avg_latency_ms + 'ms'"></span>
                </div>
                <div class="grid grid-cols-3 gap-2 text-xs mb-2">
                  <div><span class="text-slate-500">Runs:</span> <span class="text-slate-300" x-text="stats.runs"></span></div>
                  <div><span class="text-slate-500">Valid JSON:</span> <span class="text-slate-300" x-text="stats.valid_json"></span></div>
                  <div><span class="text-slate-500">Passed All:</span> <span class="text-slate-300" x-text="stats.passed_all_checks"></span></div>
                  <div><span class="text-slate-500">Numbers:</span> <span class="text-slate-300" x-text="stats.passed_number_check"></span></div>
                  <div><span class="text-slate-500">Metrics:</span> <span class="text-slate-300" x-text="stats.passed_metric_check"></span></div>
                  <div><span class="text-slate-500">Coverage:</span> <span class="text-slate-300" x-text="stats.passed_coverage_check"></span></div>
                </div>
                <div class="text-[11px] text-slate-500">
                  Tokens: <span class="text-slate-400" x-text="stats.total_input_tokens + ' in / ' + stats.total_output_tokens + ' out'"></span>
                </div>
                <template x-for="(err, ei) in stats.errors || []" :key="ei">
                  <div class="text-[11px] text-red-400 mt-1" x-text="'Run ' + err.run + ': ' + err.error"></div>
                </template>
              </div>
            </template>
          </div>
        </div>

      </div>
    </div>
  </div>

  <!-- Toast -->
  <div x-show="toastVisible" x-transition class="fixed bottom-6 right-6 bg-accent text-white text-sm px-4 py-2 rounded-lg shadow-lg z-50" x-text="toastMsg"></div>
</div>

<script>
function testerApp() {
  return {
    orgId: localStorage.getItem('dta_org') || 'org_demo',
    userId: 'tester_1',
    sessionId: 'tester-' + Date.now(),
    input: '',
    sending: false,
    messages: [],
    queries: [],
    auditEntries: [],
    activeTab: 'dashboards',
    contextTabs: [
      { id: 'dashboards', label: 'Dashboards' },
      { id: 'memos', label: 'Memos' },
      { id: 'queries', label: 'Queries' },
      { id: 'profile', label: 'Profile' },
      { id: 'audit', label: 'Audit' },
      { id: 'llm_usage', label: 'LLM Usage' },
      { id: 'comparison', label: 'Model Comparison' },
      { id: 'my_feedback', label: 'My Feedback' },
    ],
    status: { llm_ok: false, llm_model: '', bq_ok: false, bq_mode: '...', mb_ok: false, mb_mode: '...' },
    artifacts: { dashboards: [], memos: [], profiles: [] },
    memoDetail: null,
    profileDetail: null,
    llmUsage: null,
    llmBudget: null,
    evalResults: null,
    evalRunning: false,
    feedbackCount: 0,
    myFeedbackItems: [],
    toastMsg: '',
    toastVisible: false,

    hdrs() {
      return { 'Content-Type': 'application/json', 'X-Tenant-Id': this.orgId, 'X-User-Role': 'admin' };
    },

    persistOrg() { localStorage.setItem('dta_org', this.orgId); },

    async init() {
      this.persistOrg();
      await this.refreshStatus();
      await this.refreshArtifacts();
    },

    async refreshStatus() {
      try {
        const [readyRes, llmRes] = await Promise.all([fetch('/ready'), fetch('/api/v1/llm/status')]);
        const ready = await readyRes.json();
        const llm = await llmRes.json();
        this.status.bq_ok = ready.checks?.bigquery?.ok ?? false;
        this.status.bq_mode = ready.checks?.bigquery?.mode ?? 'unknown';
        this.status.mb_ok = ready.checks?.metabase?.ok ?? false;
        this.status.mb_mode = ready.checks?.metabase?.mode ?? 'unknown';
        this.status.llm_ok = llm.configured ?? false;
        this.status.llm_model = llm.model || (llm.mode === 'fallback' ? 'fallback' : 'n/a');
      } catch (e) { console.error('Status fetch failed', e); }
    },

    async refreshArtifacts() {
      try {
        const res = await fetch('/api/v1/artifacts?org_id=' + encodeURIComponent(this.orgId), { headers: this.hdrs() });
        const data = await res.json();
        const items = data.items || [];
        this.artifacts.dashboards = items.filter(i => i.type === 'dashboard');
        this.artifacts.memos = items.filter(i => i.type === 'memo');
        this.artifacts.profiles = items.filter(i => i.type === 'profile');
      } catch (e) { console.error('Artifacts fetch failed', e); }
    },

    async loadMemoDetail(id) {
      try {
        const res = await fetch('/api/v1/artifacts/' + encodeURIComponent(id) + '?org_id=' + encodeURIComponent(this.orgId), { headers: this.hdrs() });
        const data = await res.json();
        this.memoDetail = JSON.stringify(data, null, 2);
      } catch (e) { this.memoDetail = 'Failed to load: ' + e; }
    },

    async loadProfileDetail(id) {
      try {
        const res = await fetch('/api/v1/artifacts/' + encodeURIComponent(id) + '?org_id=' + encodeURIComponent(this.orgId), { headers: this.hdrs() });
        const data = await res.json();
        this.profileDetail = JSON.stringify(data, null, 2);
      } catch (e) { this.profileDetail = 'Failed to load: ' + e; }
    },

    async loadLLMUsage() {
      const org = encodeURIComponent(this.orgId);
      try {
        const [uRes, bRes] = await Promise.all([
          fetch('/api/v1/llm/usage?org_id=' + org, { headers: this.hdrs() }),
          fetch('/api/v1/llm/budget?org_id=' + org, { headers: this.hdrs() }),
        ]);
        this.llmUsage = await uRes.json();
        this.llmBudget = await bRes.json();
      } catch (e) { console.error('LLM usage fetch failed', e); }
    },

    providerColor(name) {
      if (!name) return 'text-slate-400';
      const n = name.toLowerCase();
      if (n.includes('grok') || n.includes('xai')) return 'provider-grok';
      if (n.includes('gpt') || n.includes('openai')) return 'provider-gpt';
      if (n.includes('claude') || n.includes('anthropic')) return 'provider-claude';
      return 'text-slate-400';
    },

    sendGuided(text) { this.input = text; this.sendMessage(); },

    async sendMessage() {
      const text = this.input.trim();
      if (!text || this.sending) return;
      this.input = '';
      this.messages.push({ role: 'user', text });
      const loadIdx = this.messages.length;
      this.messages.push({ role: 'loading' });
      this.sending = true;
      this.scrollChat();

      const t0 = performance.now();
      try {
        const res = await fetch('/api/v1/chat/run', {
          method: 'POST',
          headers: this.hdrs(),
          body: JSON.stringify({ org_id: this.orgId, user_id: this.userId, session_id: this.sessionId, message: text }),
        });
        const elapsed = Math.round(performance.now() - t0);
        const data = await res.json();

        if (!res.ok) {
          this.messages[loadIdx] = { role: 'error', text: data.detail || JSON.stringify(data), originalText: text };
          this.sending = false;
          return;
        }

        const msg = {
          role: 'assistant',
          text: data.summary || 'Done.',
          data: data.data,
          dataFormatted: data.data ? JSON.stringify(data.data, null, 2) : null,
          provider: data.meta?.provider || '',
          latency: data.meta?.latency_ms || elapsed,
          cost: data.meta?.estimated_cost ? data.meta.estimated_cost.toFixed(4) : null,
          responseType: data.response_type,
          meta: data.meta || {},
          feedbackType: null,
          showFeedbackForm: false,
          feedbackComment: '',
          artifactId: data.meta?.artifact_id || null,
          artifactVersion: data.meta?.artifact_version || null,
        };
        this.messages[loadIdx] = msg;

        // Populate queries tab
        if (data.response_type === 'query_result' && data.data) {
          this.queries.push({
            sql: data.data.sql || data.data.query || null,
            bytes_scanned: data.data.bytes_scanned || null,
            cost: data.data.query_cost || null,
          });
        }

        // Add audit entry
        this.auditEntries.unshift({
          event_type: 'chat_run',
          timestamp: new Date().toISOString(),
          detail: JSON.stringify({ response_type: data.response_type, intent: data.meta?.intent_action }, null, 2),
        });

        await this.refreshArtifacts();
      } catch (e) {
        this.messages[loadIdx] = { role: 'error', text: 'Request failed: ' + String(e), originalText: text };
      }
      this.sending = false;
      this.scrollChat();
    },

    retryMessage(msg) {
      if (msg.originalText) { this.input = msg.originalText; this.sendMessage(); }
    },

    showToast(text) {
      this.toastMsg = text;
      this.toastVisible = true;
      setTimeout(() => { this.toastVisible = false; }, 3000);
    },

    async loadMyFeedback() {
      try {
        const [revRes, sumRes] = await Promise.all([
          fetch('/api/v1/feedback/review?org_id=' + encodeURIComponent(this.orgId), { headers: this.hdrs() }),
          fetch('/api/v1/feedback/provider-summary?org_id=' + encodeURIComponent(this.orgId), { headers: this.hdrs() }),
        ]);
        const data = await revRes.json();
        this.myFeedbackItems = data.items || [];
        this.feedbackCount = this.myFeedbackItems.length;
        this.providerSummary = await sumRes.json();
      } catch (e) { console.error('Load feedback failed', e); }
    },

    async submitFeedback(msg, type) {
      msg.feedbackType = type;
      if (type === 'positive') msg.showFeedbackForm = false;
      const recentMsgs = this.messages.filter(m => m.role === 'user' || m.role === 'assistant').slice(-6).map(m => ({ role: m.role, text: (m.text || '').slice(0, 200) }));
      try {
        await fetch('/api/v1/feedback', {
          method: 'POST',
          headers: this.hdrs(),
          body: JSON.stringify({
            tenant_id: this.orgId,
            user_id: this.userId,
            artifact_id: msg.artifactId || 'chat_response',
            artifact_version: msg.artifactVersion || 1,
            artifact_type: msg.responseType || 'chat',
            feedback_type: type,
            comment: msg.feedbackComment || null,
            prompt_hash: msg.meta?.prompt_hash || null,
            provider: msg.provider || null,
            model: msg.meta?.model || null,
            session_id: this.sessionId,
            was_fallback: msg.meta?.was_fallback || false,
            conversation_context: recentMsgs,
            channel: 'tester_app',
          }),
        });
        this.feedbackCount++;
        this.showToast('Feedback submitted');
      } catch (e) { console.error('Feedback failed', e); }
    },

    async submitFeedbackComment(msg) {
      msg.showFeedbackForm = false;
      await this.submitFeedback(msg, 'negative');
    },

    async compareModel(msg) {
      this.activeTab = 'comparison';
      await this.runMemoEval();
    },

    async runMemoEval() {
      this.evalRunning = true;
      try {
        const res = await fetch('/api/v1/llm/evaluate-memo', {
          method: 'POST',
          headers: this.hdrs(),
          body: JSON.stringify({ org_id: this.orgId }),
        });
        const data = await res.json();
        this.evalResults = data.results || null;
      } catch (e) { console.error('Eval failed', e); }
      this.evalRunning = false;
    },

    scrollChat() {
      this.$nextTick(() => {
        const el = document.getElementById('chatMessages');
        if (el) el.scrollTop = el.scrollHeight;
      });
    },
  };
}
</script>
</body>
</html>
"""


def _demo_memo_packet() -> dict:
    """Standard demo packet for memo evaluation when no real data is available."""
    return {
        "generated_at": "2026-02-17T00:00:00Z",
        "time_window": {
            "current": {"start": "2026-02-10", "end": "2026-02-16"},
            "previous": {"start": "2026-02-03", "end": "2026-02-09"},
            "timezone": "America/New_York",
        },
        "kpis": [
            {
                "metric_name": "DAU",
                "current_value": 12450,
                "previous_value": 11200,
                "delta_absolute": 1250,
                "delta_percent": 11.16,
                "significance": "notable",
                "query_hash": "q_dau",
            },
            {
                "metric_name": "Revenue",
                "current_value": 84320.50,
                "previous_value": 78900.00,
                "delta_absolute": 5420.50,
                "delta_percent": 6.87,
                "significance": "normal",
                "query_hash": "q_revenue",
            },
            {
                "metric_name": "Conversion Rate",
                "current_value": 3.42,
                "previous_value": 2.91,
                "delta_absolute": 0.51,
                "delta_percent": 17.53,
                "significance": "notable",
                "query_hash": "q_conversion",
            },
        ],
        "top_segments": [
            {"segment": "organic_search", "delta_contribution_pct": 42.3},
            {"segment": "paid_social", "delta_contribution_pct": 28.1},
        ],
        "anomaly_notes": ["analytics.events table had 8-hour delay on 2026-02-14"],
    }
