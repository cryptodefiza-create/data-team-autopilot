from __future__ import annotations

import hashlib
from datetime import datetime

from data_autopilot.agents.contracts import AgentPlan, StepResult
from data_autopilot.tools.executors.mock_query_executor import MockQueryExecutor


class Executor:
    def __init__(self, query_executor: MockQueryExecutor, max_retries: int = 3) -> None:
        self.query_executor = query_executor
        self.max_retries = max_retries

    def run(self, plan: AgentPlan) -> list[StepResult]:
        results: list[StepResult] = []
        for step in plan.steps:
            start = datetime.utcnow()
            retries = 0
            output: dict = {}
            error: str | None = None
            status = "success"

            while True:
                try:
                    if step.tool == "execute_query":
                        output = self.query_executor.execute(f"step_{step.step_id}", step.inputs["sql"])
                    break
                except RuntimeError as exc:
                    error = str(exc)
                    if error in {"transient_error", "timeout"} and retries < self.max_retries:
                        retries += 1
                        continue
                    status = "failed"
                    break

            end = datetime.utcnow()
            digest = hashlib.sha256(repr(output).encode("utf-8")).hexdigest()
            results.append(
                StepResult(
                    step_name=step.tool,
                    status=status,
                    output=output,
                    output_hash=digest,
                    started_at=start,
                    finished_at=end,
                    retry_count=retries,
                    error=error,
                )
            )
        return results
