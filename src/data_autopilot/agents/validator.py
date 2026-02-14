from data_autopilot.agents.contracts import AgentPlan


class PlanValidator:
    def validate(self, plan: AgentPlan) -> tuple[bool, list[str]]:
        errors: list[str] = []
        for step in plan.steps:
            if not step.tool:
                errors.append("Missing tool in step")
            if step.tool == "execute_query" and "sql" not in step.inputs:
                errors.append("execute_query missing sql")
        return (len(errors) == 0, errors)
