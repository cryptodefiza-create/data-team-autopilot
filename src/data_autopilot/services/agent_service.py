from datetime import datetime
import hashlib

from sqlalchemy.orm import Session

from data_autopilot.agents.composer import compose
from data_autopilot.agents.critic import Critic
from data_autopilot.agents.executor import Executor
from data_autopilot.agents.contracts import StepResult
from data_autopilot.agents.planner import Planner
from data_autopilot.agents.validator import PlanValidator
from data_autopilot.config.settings import get_settings
from data_autopilot.services.audit import AuditService
from data_autopilot.services.cost_limiter import SlidingWindowCostLimiter
from data_autopilot.services.query_service import QueryService
from data_autopilot.services.sql_safety import SqlSafetyEngine
from data_autopilot.tools.executors.mock_query_executor import MockQueryExecutor


class AgentService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.planner = Planner()
        self.validator = PlanValidator()
        self.critic = Critic(SqlSafetyEngine(), SlidingWindowCostLimiter())
        self.executor = Executor(MockQueryExecutor())
        self.query_service = QueryService()
        self.audit = AuditService()

    @staticmethod
    def _hash_output(payload: dict) -> str:
        return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()

    def _run_real_query_path(self, db: Session, org_id: str, plan) -> list[StepResult]:
        results: list[StepResult] = []
        for step in plan.steps:
            if step.tool != "execute_query":
                continue
            started = datetime.utcnow()
            sql = str(step.inputs.get("sql", ""))
            preview = self.query_service.preview(db, tenant_id=org_id, sql=sql)
            status = str(preview.get("status", "blocked"))
            if status == "blocked":
                output = {
                    "status": "blocked",
                    "reasons": preview.get("reasons", ["Query blocked"]),
                    "gate": {"approval_required": False},
                }
                finished = datetime.utcnow()
                results.append(
                    StepResult(
                        step_name=step.tool,
                        status="failed",
                        output=output,
                        output_hash=self._hash_output(output),
                        started_at=started,
                        finished_at=finished,
                        retry_count=0,
                        error="blocked",
                    )
                )
                continue
            if status == "approval_required":
                output = {
                    "status": "approval_required",
                    "preview_id": preview.get("preview_id"),
                    "estimated_bytes": preview.get("estimated_bytes", 0),
                    "estimated_cost_usd": preview.get("estimated_cost_usd", 0),
                    "requires_approval": True,
                    "approval": {
                        "required": True,
                        "endpoint_preview": "/api/v1/queries/preview",
                        "endpoint_approve_run": "/api/v1/queries/approve-run",
                        "message": "Preview this query, then approve and run.",
                    },
                }
                finished = datetime.utcnow()
                results.append(
                    StepResult(
                        step_name=step.tool,
                        status="failed",
                        output=output,
                        output_hash=self._hash_output(output),
                        started_at=started,
                        finished_at=finished,
                        retry_count=0,
                        error="approval_required",
                    )
                )
                continue
            execute = self.query_service.approve_and_run(
                db,
                tenant_id=org_id,
                preview_id=str(preview["preview_id"]),
            )
            finished = datetime.utcnow()
            results.append(
                StepResult(
                    step_name=step.tool,
                    status="success" if execute.get("status") == "executed" else "failed",
                    output=execute,
                    output_hash=self._hash_output(execute),
                    started_at=started,
                    finished_at=finished,
                    retry_count=0,
                    error=None if execute.get("status") == "executed" else str(execute.get("status")),
                )
            )
        return results

    def run(self, db: Session, org_id: str, user_id: str, message: str) -> dict:
        plan = self.planner.plan(message)
        valid, errors = self.validator.validate(plan)
        if not valid:
            return {"response_type": "error", "summary": "Plan validation failed", "data": {"errors": errors}, "warnings": []}

        allowed, reasons, checked_plan, gate_meta = self.critic.pre_execute(org_id, plan)
        self.audit.log(db, org_id, "security_gate_decision", {"allowed": allowed, "reasons": reasons, "gate_meta": gate_meta})
        if not allowed:
            data = {"reasons": reasons, "gate": gate_meta}
            if gate_meta.get("approval_required"):
                data["approval"] = {
                    "required": True,
                    "endpoint_preview": "/api/v1/queries/preview",
                    "endpoint_approve_run": "/api/v1/queries/approve-run",
                    "message": "Preview this query, then approve and run.",
                }
            return {
                "response_type": "blocked",
                "summary": "Query blocked by safety/cost gates",
                "data": data,
                "warnings": [],
            }

        if self.settings.allow_real_query_execution:
            results = self._run_real_query_path(db=db, org_id=org_id, plan=checked_plan)
        else:
            results = self.executor.run(checked_plan)

        for result in results:
            self.audit.log(
                db,
                org_id,
                "tool_invocation",
                {
                    "tool": result.step_name,
                    "status": result.status,
                    "output_hash": result.output_hash,
                    "retry_count": result.retry_count,
                    "error": result.error,
                },
            )

        warnings = []
        if results:
            warnings = self.critic.post_execute(results[0].output)

        return compose(results, warnings)
