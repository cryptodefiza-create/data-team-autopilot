from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


@dataclass
class PlanStep:
    step_id: int
    tool: str
    inputs: dict[str, Any]
    risk_flags: list[str] = field(default_factory=list)


@dataclass
class AgentPlan:
    goal: str
    steps: list[PlanStep]
    required_approvals: list[str] = field(default_factory=list)


@dataclass
class StepResult:
    step_name: str
    status: Literal["success", "failed", "skipped"]
    output: dict[str, Any]
    output_hash: str
    started_at: datetime
    finished_at: datetime
    retry_count: int = 0
    error: str | None = None
