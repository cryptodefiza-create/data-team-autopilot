from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Any


@dataclass
class WorkflowStepState:
    step_name: str
    status: str
    output: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    error: str | None = None


@dataclass
class WorkflowRun:
    workflow_id: str
    org_id: str
    status: str
    steps: list[WorkflowStepState]
    started_at: datetime
    finished_at: datetime | None = None


class InMemoryWorkflowStore:
    def __init__(self) -> None:
        self.by_key: dict[str, WorkflowStepState] = {}

    def key(self, org_id: str, workflow_id: str, step_name: str, payload: dict[str, Any]) -> str:
        payload_hash = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()
        return f"{org_id}:{workflow_id}:{step_name}:{payload_hash}"

    def get(self, key: str) -> WorkflowStepState | None:
        return self.by_key.get(key)

    def put(self, key: str, state: WorkflowStepState) -> None:
        self.by_key[key] = state
