"""A/B evaluation service for comparing LLM providers.

Runs the same prompt against the primary provider and all enabled evaluation
providers in parallel. Stores comparison results in the database for later
analysis during the tester phase.
"""
from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from data_autopilot.services.llm_client import (
    LLMClient,
    LLMResult,
    _call_provider,
    get_eval_providers,
)


class LLMEvalService:
    """Orchestrates parallel LLM evaluation and stores results."""

    def __init__(self, primary_client: LLMClient | None = None) -> None:
        self.primary = primary_client or LLMClient()

    def evaluate(
        self,
        db: Session,
        *,
        tenant_id: str,
        task_type: str,
        system_prompt: str,
        user_prompt: str,
    ) -> EvalRun:
        """Run primary + all eval providers and store results.

        Returns the EvalRun with primary result and all comparison results.
        The primary result is always used for the actual response — eval
        providers are fire-and-forget comparisons.
        """
        eval_providers = get_eval_providers()
        run_id = f"eval_{uuid.uuid4().hex[:12]}"
        started_at = datetime.now(timezone.utc)

        # Always call primary
        primary_result = self.primary.generate_json_with_meta(system_prompt, user_prompt)

        # Run eval providers in parallel (non-blocking for the user)
        eval_results: list[LLMResult] = []
        if eval_providers:
            with ThreadPoolExecutor(max_workers=len(eval_providers)) as pool:
                futures = {
                    pool.submit(_call_provider, p, system_prompt, user_prompt): p
                    for p in eval_providers
                }
                for fut in as_completed(futures):
                    eval_results.append(fut.result())

        run = EvalRun(
            run_id=run_id,
            tenant_id=tenant_id,
            task_type=task_type,
            started_at=started_at,
            primary=primary_result,
            evaluations=eval_results,
        )

        _store_eval_run(db, run)
        return run

    def evaluate_primary_only(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> LLMResult:
        """Call just the primary provider (no eval, no DB storage)."""
        return self.primary.generate_json_with_meta(system_prompt, user_prompt)


class EvalRun:
    """Container for a single evaluation run across providers."""

    __slots__ = ("run_id", "tenant_id", "task_type", "started_at", "primary", "evaluations")

    def __init__(
        self,
        run_id: str,
        tenant_id: str,
        task_type: str,
        started_at: datetime,
        primary: LLMResult,
        evaluations: list[LLMResult],
    ) -> None:
        self.run_id = run_id
        self.tenant_id = tenant_id
        self.task_type = task_type
        self.started_at = started_at
        self.primary = primary
        self.evaluations = evaluations

    @property
    def all_results(self) -> list[LLMResult]:
        return [self.primary] + self.evaluations

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "tenant_id": self.tenant_id,
            "task_type": self.task_type,
            "started_at": self.started_at.isoformat(),
            "primary": _result_to_dict(self.primary),
            "evaluations": [_result_to_dict(r) for r in self.evaluations],
        }


def _result_to_dict(result: LLMResult) -> dict:
    return {
        "provider_name": result.provider_name,
        "model": result.model,
        "latency_ms": result.latency_ms,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "succeeded": result.succeeded,
        "error": result.error,
        "content_keys": list(result.content.keys()) if result.content else [],
    }


def _store_eval_run(db: Session, run: EvalRun) -> None:
    """Persist evaluation run to the audit log for later analysis."""
    from data_autopilot.services.audit import AuditService

    audit = AuditService()
    audit.log(
        db,
        tenant_id=run.tenant_id,
        event_type="llm_eval_run",
        payload=run.to_dict(),
    )
