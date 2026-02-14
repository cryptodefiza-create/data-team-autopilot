from data_autopilot.agents.contracts import StepResult


class Composer:
    def compose(self, results: list[StepResult], warnings: list[str]) -> dict:
        if not results:
            return {"response_type": "error", "summary": "No steps executed", "data": {}, "warnings": warnings}

        first = results[0]
        if first.status != "success":
            return {
                "response_type": "partial_failure",
                "summary": f"Step failed: {first.error}",
                "data": {"step": first.step_name, "retry_count": first.retry_count},
                "warnings": warnings,
            }

        return {
            "response_type": "query_result",
            "summary": "Query completed",
            "data": first.output,
            "warnings": warnings,
        }
