from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: Literal["ok"]
    app: str


class AgentRequest(BaseModel):
    org_id: str
    user_id: str
    message: str = Field(min_length=1)
    session_id: str


class AgentResponse(BaseModel):
    response_type: str
    summary: str
    data: dict[str, Any]
    warnings: list[str] = Field(default_factory=list)


class FeedbackRequest(BaseModel):
    tenant_id: str
    user_id: str
    artifact_id: str
    artifact_version: int
    artifact_type: str
    feedback_type: Literal["positive", "negative"]
    comment: str | None = None
    prompt_hash: str | None = None
    tool_inputs_hash: str | None = None


class FeedbackResponse(BaseModel):
    id: str
    created_at: datetime


class ConnectorRequest(BaseModel):
    org_id: str
    service_account_json: dict[str, Any] = Field(default_factory=dict)


class ConnectorResponse(BaseModel):
    connection_id: str
    status: str
