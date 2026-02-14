from data_autopilot.agents.contracts import AgentPlan
from data_autopilot.config.settings import get_settings
from data_autopilot.services.cost_guard import CostGuard
from data_autopilot.services.sql_safety import SqlSafetyEngine


class Critic:
    def __init__(self, safety: SqlSafetyEngine, cost_guard: CostGuard) -> None:
        self.safety = safety
        self.cost_guard = cost_guard
        self.settings = get_settings()

    def pre_execute(self, org_id: str, plan: AgentPlan) -> tuple[bool, list[str], AgentPlan, dict]:
        reasons: list[str] = []
        gate_meta: dict = {
            "approval_required": False,
            "next_action": None,
            "estimated_bytes": 0,
            "estimated_cost_usd": 0.0,
        }
        for step in plan.steps:
            if step.tool != "execute_query":
                continue
            decision = self.safety.evaluate(step.inputs["sql"])
            if not decision.allowed:
                reasons.extend(decision.reasons)
                gate_meta["next_action"] = "revise_query"
                return (False, reasons, plan, gate_meta)
            if decision.rewritten_sql:
                step.inputs["sql"] = decision.rewritten_sql

            est_bytes = min(len(step.inputs["sql"]) * 2048, self.settings.per_query_max_bytes_with_approval + 1)
            gate_meta["estimated_bytes"] = est_bytes
            gate_meta["estimated_cost_usd"] = round((est_bytes / (1024**4)) * 5.0, 4)
            if est_bytes > self.settings.per_query_max_bytes_with_approval:
                reasons.append("Query exceeds hard max bytes with approval")
                gate_meta["next_action"] = "narrow_scope"
                return (False, reasons, plan, gate_meta)
            if est_bytes > self.settings.per_query_max_bytes:
                reasons.append("Query exceeds per-query limit and requires approval")
                gate_meta["approval_required"] = True
                gate_meta["next_action"] = "preview_then_approve"
                return (False, reasons, plan, gate_meta)

            budget = self.cost_guard.check(org_id, est_bytes)
            if not budget.allowed:
                reasons.append("Hourly budget exceeded")
                gate_meta["next_action"] = "wait_or_reduce_cost"
                return (False, reasons, plan, gate_meta)
            step.inputs["estimated_bytes"] = est_bytes
        return (True, reasons, plan, gate_meta)

    def post_execute(self, output: dict) -> list[str]:
        warnings: list[str] = []
        rows = output.get("rows", [])
        if isinstance(rows, list) and len(rows) == 0:
            warnings.append("No data returned")
        return warnings
