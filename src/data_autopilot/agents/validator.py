from data_autopilot.agents.contracts import AgentPlan
from data_autopilot.services.sql_safety import SqlSafetyEngine


class PlanValidator:
    def __init__(self) -> None:
        self.safety = SqlSafetyEngine()

    def validate(self, plan: AgentPlan) -> tuple[bool, list[str]]:
        errors: list[str] = []
        allowed_tools = {"execute_query"}
        for step in plan.steps:
            if not step.tool:
                errors.append("Missing tool in step")
                continue
            if step.tool not in allowed_tools:
                errors.append(f"Unsupported tool: {step.tool}")
                continue
            if step.tool == "execute_query" and "sql" not in step.inputs:
                errors.append("execute_query missing sql")
                continue
            sql = step.inputs.get("sql")
            if not isinstance(sql, str) or not sql.strip():
                errors.append("execute_query sql must be a non-empty string")
                continue
            decision = self.safety.evaluate(sql)
            if not decision.allowed:
                errors.extend(decision.reasons)
        return (len(errors) == 0, errors)
