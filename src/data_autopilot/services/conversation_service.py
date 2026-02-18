from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from data_autopilot.agents.planner import Planner
from data_autopilot.services.degradation_service import DegradationService
from data_autopilot.services.llm_client import LLMClient
from data_autopilot.services.llm_cost_service import LLMCostService
from data_autopilot.services.query_service import QueryService
from data_autopilot.services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)


class ConversationService:
    def __init__(self) -> None:
        self.llm = LLMClient()
        self.planner = Planner(llm_client=self.llm)
        self.query_service = QueryService()
        self.workflow_service = WorkflowService()
        self.degradation = DegradationService()
        self.cost_service = LLMCostService()

    @staticmethod
    def _fallback_action(message: str) -> str:
        text = message.lower()
        if any(token in text for token in {"dashboard", "chart", "visual", "kpi board"}):
            return "dashboard"
        if any(token in text for token in {"profile", "catalog", "schema", "introspect"}):
            return "profile"
        if any(token in text for token in {"memo", "weekly", "summary", "report"}):
            return "memo"
        return "query"

    def _interpret(self, db: Session, tenant_id: str, message: str) -> dict:
        action = self._fallback_action(message)
        sql = ""
        if self.llm.is_configured():
            system_prompt = (
                "You route user requests for a data agent. "
                "Return JSON with keys action, sql, reason. "
                "action must be one of: query, profile, dashboard, memo. "
                "Only provide sql when action=query."
            )
            user_prompt = f"Request: {message}"
            try:
                result = self.llm.generate_json_with_meta(system_prompt=system_prompt, user_prompt=user_prompt)
                self.cost_service.record(
                    db, tenant_id=tenant_id, result=result, task_type="intent_classification",
                )
                if not result.succeeded:
                    raise RuntimeError(result.error)
                parsed = result.content
                candidate = str(parsed.get("action", "")).strip().lower()
                if candidate in {"query", "profile", "dashboard", "memo"}:
                    action = candidate
                sql_val = parsed.get("sql", "")
                if isinstance(sql_val, str):
                    sql = sql_val.strip()
                return {"action": action, "sql": sql, "reason": str(parsed.get("reason", "")).strip()}
            except Exception as exc:
                logger.error("LLM intent classification failed: %s", exc, exc_info=True)
        return {"action": action, "sql": sql, "reason": "fallback_intent_classifier"}

    def _query_response(self, db: Session, tenant_id: str, message: str, suggested_sql: str) -> dict:
        sql = suggested_sql
        if not sql:
            plan = self.planner.plan(message)
            step = plan.steps[0] if plan.steps else None
            if step is None or step.tool != "execute_query":
                return {
                    "response_type": "error",
                    "summary": "Could not build a query plan.",
                    "data": {},
                    "warnings": ["planner_failed"],
                }
            sql = str(step.inputs.get("sql", "")).strip()
        if not sql:
            return {"response_type": "error", "summary": "Generated SQL was empty.", "data": {}, "warnings": []}

        try:
            preview = self.query_service.preview(db, tenant_id=tenant_id, sql=sql)
        except Exception as exc:
            logger.error("Query preview failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Query execution failed: {exc}",
                "data": {"sql": sql},
                "warnings": ["query_execution_error"],
            }
        status = str(preview.get("status", "blocked"))
        if status == "blocked":
            return {
                "response_type": "blocked",
                "summary": "Query blocked by safety or budget gates.",
                "data": {"preview": preview, "sql": sql},
                "warnings": [],
            }
        if status == "approval_required":
            return {
                "response_type": "approval_required",
                "summary": "Query needs approval before execution.",
                "data": {
                    "preview_id": preview.get("preview_id"),
                    "estimated_bytes": preview.get("estimated_bytes"),
                    "estimated_cost_usd": preview.get("estimated_cost_usd"),
                    "sql": sql,
                    "instructions": "Approve via /api/v1/queries/approve-run with preview_id.",
                },
                "warnings": [],
            }

        try:
            execute = self.query_service.approve_and_run(db, tenant_id=tenant_id, preview_id=str(preview["preview_id"]))
        except Exception as exc:
            logger.error("Query execution failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Query execution failed: {exc}",
                "data": {"sql": sql},
                "warnings": ["query_execution_error"],
            }
        return {
            "response_type": "query_result",
            "summary": "Query completed.",
            "data": execute,
            "warnings": [],
        }

    def respond(self, db: Session, tenant_id: str, user_id: str, message: str) -> dict:
        interpreted = self._interpret(db, tenant_id, message)
        action = interpreted["action"]
        result: dict
        if action == "query":
            result = self._query_response(
                db=db,
                tenant_id=tenant_id,
                message=message,
                suggested_sql=interpreted.get("sql", ""),
            )
        elif action == "profile":
            if not self.workflow_service.has_capacity(db, tenant_id=tenant_id, workflow_type="profile"):
                queued = self.degradation.enqueue(
                    db, tenant_id=tenant_id, workflow_type="profile", payload={"org_id": tenant_id}, reason="concurrency_limit"
                )
                result = {
                    "response_type": "queued",
                    "summary": "Profiling is queued due to concurrency limits.",
                    "data": queued,
                    "warnings": [],
                }
            else:
                try:
                    flow = self.workflow_service.run_profile_flow(db, tenant_id=tenant_id)
                    result = {"response_type": "workflow_result", "summary": "Profile workflow completed.", "data": flow, "warnings": []}
                except Exception as exc:
                    logger.error("Profile workflow failed: %s", exc, exc_info=True)
                    result = {"response_type": "error", "summary": f"Profile workflow failed: {exc}", "data": {}, "warnings": ["workflow_error"]}
        elif action == "dashboard":
            if not self.workflow_service.has_capacity(db, tenant_id=tenant_id, workflow_type="dashboard"):
                queued = self.degradation.enqueue(
                    db, tenant_id=tenant_id, workflow_type="dashboard", payload={"org_id": tenant_id}, reason="concurrency_limit"
                )
                result = {
                    "response_type": "queued",
                    "summary": "Dashboard generation is queued due to concurrency limits.",
                    "data": queued,
                    "warnings": [],
                }
            else:
                try:
                    flow = self.workflow_service.run_dashboard_flow(db, tenant_id=tenant_id)
                    result = {
                        "response_type": "workflow_result",
                        "summary": "Dashboard generation completed.",
                        "data": flow,
                        "warnings": [],
                    }
                except Exception as exc:
                    logger.error("Dashboard workflow failed: %s", exc, exc_info=True)
                    result = {"response_type": "error", "summary": f"Dashboard workflow failed: {exc}", "data": {}, "warnings": ["workflow_error"]}
        else:
            if not self.workflow_service.has_capacity(db, tenant_id=tenant_id, workflow_type="memo"):
                queued = self.degradation.enqueue(
                    db, tenant_id=tenant_id, workflow_type="memo", payload={"org_id": tenant_id}, reason="concurrency_limit"
                )
                result = {
                    "response_type": "queued",
                    "summary": "Memo generation is queued due to concurrency limits.",
                    "data": queued,
                    "warnings": [],
                }
            else:
                try:
                    flow = self.workflow_service.run_memo_flow(db, tenant_id=tenant_id)
                    result = {"response_type": "workflow_result", "summary": "Weekly memo generated.", "data": flow, "warnings": []}
                except Exception as exc:
                    logger.error("Memo workflow failed: %s", exc, exc_info=True)
                    result = {"response_type": "error", "summary": f"Memo workflow failed: {exc}", "data": {}, "warnings": ["workflow_error"]}

        result["meta"] = {
            "intent_action": action,
            "llm_configured": self.llm.is_configured(),
            "interpreter_reason": interpreted.get("reason", ""),
            "user_id": user_id,
        }
        return result
