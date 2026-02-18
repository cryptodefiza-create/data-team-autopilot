from __future__ import annotations

import logging

from data_autopilot.agents.contracts import AgentPlan, PlanStep
from data_autopilot.services.llm_client import LLMClient

logger = logging.getLogger(__name__)


class Planner:
    def __init__(self, llm_client: LLMClient | None = None) -> None:
        self.llm = llm_client or LLMClient()

    def _fallback_plan(self, message: str) -> AgentPlan:
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

    def plan(self, message: str) -> AgentPlan:
        if not self.llm.is_configured():
            return self._fallback_plan(message)

        system_prompt = (
            "You are a data SQL planner. Return only JSON with keys: goal, sql. "
            "Generate one safe SELECT query for BigQuery. Never use DDL or DML. "
            "Prefer analytics.events and analytics.orders if needed. "
            "Include a LIMIT when the result can be large."
        )
        user_prompt = f"User request: {message}"
        try:
            planned = self.llm.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            sql = str(planned.get("sql", "")).strip()
            if not sql:
                return self._fallback_plan(message)
            goal = str(planned.get("goal", "Respond to user query")).strip() or "Respond to user query"
            return AgentPlan(
                goal=goal,
                steps=[PlanStep(step_id=1, tool="execute_query", inputs={"sql": sql})],
            )
        except Exception:
            logger.warning("LLM planning failed, using fallback", exc_info=True)
            return self._fallback_plan(message)
