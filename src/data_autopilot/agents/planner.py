from data_autopilot.agents.contracts import AgentPlan, PlanStep


class Planner:
    def plan(self, message: str) -> AgentPlan:
        sql = "SELECT 1 AS health_check"
        if "dau" in message.lower():
            sql = (
                "SELECT DATE(created_at) AS day, COUNT(DISTINCT user_id) AS dau "
                "FROM analytics.events GROUP BY 1"
            )

        return AgentPlan(
            goal="Respond to user query",
            steps=[PlanStep(step_id=1, tool="execute_query", inputs={"sql": sql})],
        )
