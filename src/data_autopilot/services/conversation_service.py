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
        if any(token in text for token in {"track", "monitor", "recurring", "schedule"}):
            if any(token in text for token in {
                "token", "holders", "solana", "ethereum", "wallet", "nft",
                "blockchain", "on-chain", "$", "mint", "defi",
                "tvl", "dex", "pair", "protocol", "liquidity",
            }):
                return "track_blockchain"
        if any(token in text for token in {
            "token", "holders", "solana", "ethereum", "wallet", "nft",
            "blockchain", "on-chain", "$", "mint", "defi",
            "tvl", "dex", "pair", "protocol", "liquidity",
        }):
            return "blockchain"
        if any(token in text for token in {"connect", "link", "integrate"}):
            if any(token in text for token in {"shopify", "stripe", "store", "payment"}):
                return "connect_source"
        if any(token in text for token in {
            "order", "orders", "revenue", "sales", "aov",
            "subscription", "subscriptions", "invoice", "invoices",
            "charge", "charges", "mrr", "churn",
        }):
            if any(token in text for token in {"my", "our", "store", "shop", "account"}):
                return "business_query"
        return "query"

    def _interpret(self, db: Session, tenant_id: str, message: str) -> dict:
        action = self._fallback_action(message)
        sql = ""
        if self.llm.is_configured():
            system_prompt = (
                "You route user requests for a data agent. "
                "Return JSON with keys action, sql, reason. "
                "action must be one of: query, profile, dashboard, memo, blockchain, track_blockchain, connect_source, business_query. "
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
                if candidate in {"query", "profile", "dashboard", "memo", "blockchain", "track_blockchain", "connect_source", "business_query"}:
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

    @staticmethod
    def _extract_schedule(message: str) -> str:
        text = message.lower()
        if "hourly" in text:
            return "hourly"
        if "weekly" in text:
            return "weekly"
        return "daily"

    def _track_blockchain_response(self, message: str, tenant_id: str) -> dict:
        from data_autopilot.api.state import mode1_fetcher, mode1_persistence, mode1_snapshot_pipeline
        from data_autopilot.services.mode1.persistence import TierLimitError

        try:
            # Parse the request
            request = mode1_fetcher._parser.parse(message)
            schedule = self._extract_schedule(message)
            tier = mode1_fetcher._tier

            # Ensure storage is provisioned (checks tier)
            mode1_persistence.ensure_storage(tenant_id, tier=tier)

            # Create pipeline
            pipeline = mode1_snapshot_pipeline.create(
                org_id=tenant_id, request=request, schedule=schedule
            )

            return {
                "response_type": "pipeline_created",
                "summary": (
                    f"Now tracking {request.entity.value} ({request.token or request.chain.value}) "
                    f"{schedule}. First snapshot collected."
                ),
                "data": {
                    "pipeline_id": pipeline.id,
                    "entity": pipeline.entity,
                    "schedule": pipeline.schedule,
                    "status": pipeline.status.value,
                    "run_count": pipeline.run_count,
                },
                "warnings": [],
            }
        except TierLimitError as exc:
            return {
                "response_type": "tier_blocked",
                "summary": str(exc),
                "data": {},
                "warnings": ["tier_limit"],
            }
        except Exception as exc:
            logger.error("Track blockchain failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Failed to set up tracking: {exc}",
                "data": {},
                "warnings": ["tracking_error"],
            }

    def _connect_source_response(self, message: str, tenant_id: str) -> dict:
        from data_autopilot.api.state import mode1_credential_flow

        try:
            text = message.lower()
            if "shopify" in text or "store" in text:
                return {
                    "response_type": "connect_prompt",
                    "summary": (
                        "I'll help you connect Shopify. I need two things:\n\n"
                        "1. Your store domain (e.g., my-store.myshopify.com)\n"
                        "2. An Admin API access token\n\n"
                        "To get the token:\n"
                        "Shopify Admin → Settings → Apps → Develop Apps\n"
                        "→ Create an app → Configure Admin API scopes\n"
                        "→ Enable: read_orders, read_products, read_customers\n"
                        "→ Install app → Copy the Admin API access token"
                    ),
                    "data": {"source": "shopify", "step": "awaiting_credentials"},
                    "warnings": [],
                }
            elif "stripe" in text or "payment" in text:
                return {
                    "response_type": "connect_prompt",
                    "summary": (
                        "I'll help you connect Stripe. I need your Stripe API key.\n\n"
                        "To get it:\n"
                        "Stripe Dashboard → Developers → API keys\n"
                        "→ Copy your Secret key (starts with sk_live_ or sk_test_)"
                    ),
                    "data": {"source": "stripe", "step": "awaiting_credentials"},
                    "warnings": [],
                }
            return {
                "response_type": "info",
                "summary": "I can connect Shopify or Stripe. Which would you like to set up?",
                "data": {},
                "warnings": [],
            }
        except Exception as exc:
            logger.error("Connect source failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Failed to start connection flow: {exc}",
                "data": {},
                "warnings": ["connect_error"],
            }

    def _business_query_response(self, message: str, tenant_id: str) -> dict:
        from data_autopilot.api.state import mode1_business_query

        try:
            return mode1_business_query.query(tenant_id, message)
        except Exception as exc:
            logger.error("Business query failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Business data query failed: {exc}",
                "data": {},
                "warnings": ["business_query_error"],
            }

    def _blockchain_response(self, message: str, session_id: str = "default") -> dict:
        from data_autopilot.api.state import mode1_fetcher

        try:
            return mode1_fetcher.handle(message, session_id=session_id)
        except Exception as exc:
            logger.error("Blockchain fetch failed: %s", exc, exc_info=True)
            return {
                "response_type": "error",
                "summary": f"Blockchain data fetch failed: {exc}",
                "data": {},
                "warnings": ["blockchain_error"],
            }

    def respond(self, db: Session, tenant_id: str, user_id: str, message: str) -> dict:
        interpreted = self._interpret(db, tenant_id, message)
        action = interpreted["action"]
        result: dict
        if action == "connect_source":
            result = self._connect_source_response(message, tenant_id=tenant_id)
        elif action == "business_query":
            result = self._business_query_response(message, tenant_id=tenant_id)
        elif action == "track_blockchain":
            result = self._track_blockchain_response(message, tenant_id=tenant_id)
        elif action == "blockchain":
            result = self._blockchain_response(message)
        elif action == "query":
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
