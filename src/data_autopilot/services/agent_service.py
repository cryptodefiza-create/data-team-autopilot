from sqlalchemy.orm import Session

from data_autopilot.agents.composer import Composer
from data_autopilot.agents.critic import Critic
from data_autopilot.agents.executor import Executor
from data_autopilot.agents.planner import Planner
from data_autopilot.agents.validator import PlanValidator
from data_autopilot.services.audit import AuditService
from data_autopilot.services.cost_limiter import SlidingWindowCostLimiter
from data_autopilot.services.sql_safety import SqlSafetyEngine
from data_autopilot.tools.executors.mock_query_executor import MockQueryExecutor


class AgentService:
    def __init__(self) -> None:
        self.planner = Planner()
        self.validator = PlanValidator()
        self.critic = Critic(SqlSafetyEngine(), SlidingWindowCostLimiter())
        self.executor = Executor(MockQueryExecutor())
        self.composer = Composer()
        self.audit = AuditService()

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

        return self.composer.compose(results, warnings)
