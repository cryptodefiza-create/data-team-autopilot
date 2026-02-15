from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Boolean, DateTime, Enum as SAEnum, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from data_autopilot.db.base import Base


class Role(str, Enum):
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


class ArtifactType(str, Enum):
    DASHBOARD = "dashboard"
    MEMO = "memo"
    QUERY = "query"
    PROFILE = "profile"


class FeedbackType(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"


class ConnectionType(str, Enum):
    BIGQUERY = "bigquery"


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    settings: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"), index=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[Role] = mapped_column(SAEnum(Role), default=Role.MEMBER)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Connection(Base):
    __tablename__ = "connections"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    type: Mapped[ConnectionType] = mapped_column(SAEnum(ConnectionType), default=ConnectionType.BIGQUERY)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    config_encrypted: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    type: Mapped[ArtifactType] = mapped_column(SAEnum(ArtifactType), index=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    data: Mapped[dict] = mapped_column(JSON, default=dict)
    query_hashes: Mapped[list] = mapped_column(JSON, default=list)
    stale: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ArtifactVersion(Base):
    __tablename__ = "artifact_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    artifact_id: Mapped[str] = mapped_column(String(64), index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[int] = mapped_column(Integer, index=True)
    data: Mapped[dict] = mapped_column(JSON, default=dict)
    query_hashes: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class CatalogTable(Base):
    __tablename__ = "catalog_tables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    connection_id: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(255))
    table_name: Mapped[str] = mapped_column(String(255))
    row_count_est: Mapped[int] = mapped_column(Integer, default=0)
    bytes_est: Mapped[int] = mapped_column(Integer, default=0)
    freshness_hours: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CatalogColumn(Base):
    __tablename__ = "catalog_columns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    connection_id: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(255))
    table_name: Mapped[str] = mapped_column(String(255))
    column_name: Mapped[str] = mapped_column(String(255))
    data_type: Mapped[str] = mapped_column(String(64))
    null_pct: Mapped[int] = mapped_column(Integer, default=0)
    distinct_est: Mapped[int] = mapped_column(Integer, default=0)
    is_pii: Mapped[bool] = mapped_column(Boolean, default=False)
    pii_confidence: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class Feedback(Base):
    __tablename__ = "feedback"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    artifact_id: Mapped[str] = mapped_column(String(64), index=True)
    artifact_version: Mapped[int] = mapped_column(Integer)
    artifact_type: Mapped[ArtifactType] = mapped_column(SAEnum(ArtifactType))
    feedback_type: Mapped[FeedbackType] = mapped_column(SAEnum(FeedbackType))
    comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    prompt_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    tool_inputs_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    workflow_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workflow_id: Mapped[str] = mapped_column(String(64), index=True)
    step_name: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    output: Mapped[dict] = mapped_column(JSON, default=dict)
    input_hash: Mapped[str] = mapped_column(String(64), index=True)
    output_hash: Mapped[str] = mapped_column(String(64))
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class WorkflowQueue(Base):
    __tablename__ = "workflow_queue"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    workflow_type: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    reason: Mapped[str] = mapped_column(String(255), default="service_unavailable")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error_history: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class WorkflowDeadLetter(Base):
    __tablename__ = "workflow_dead_letters"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    queue_id: Mapped[str] = mapped_column(String(64), index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    workflow_type: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    step_states: Mapped[list] = mapped_column(JSON, default=list)
    error_history: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class QueryApproval(Base):
    __tablename__ = "query_approvals"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    sql: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    estimated_bytes: Mapped[int] = mapped_column(Integer, default=0)
    estimated_cost_usd: Mapped[int] = mapped_column(Integer, default=0)
    actual_bytes: Mapped[int] = mapped_column(Integer, default=0)
    requires_approval: Mapped[bool] = mapped_column(Boolean, default=False)
    output: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    executed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class IntegrationBindingType(str, Enum):
    SLACK_TEAM = "slack_team"
    SLACK_USER = "slack_user"
    TELEGRAM_CHAT = "telegram_chat"
    TELEGRAM_USER = "telegram_user"


class IntegrationBinding(Base):
    __tablename__ = "integration_bindings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    binding_type: Mapped[IntegrationBindingType] = mapped_column(SAEnum(IntegrationBindingType), index=True)
    external_id: Mapped[str] = mapped_column(String(255), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class AlertSeverity(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class AlertStatus(str, Enum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    ESCALATED = "escalated"


class AlertEvent(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    source_type: Mapped[str] = mapped_column(String(64), default="system")
    source_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), index=True)
    title: Mapped[str] = mapped_column(String(255))
    message: Mapped[str] = mapped_column(Text)
    severity: Mapped[AlertSeverity] = mapped_column(SAEnum(AlertSeverity), index=True)
    status: Mapped[AlertStatus] = mapped_column(SAEnum(AlertStatus), default=AlertStatus.OPEN, index=True)
    escalated_count: Mapped[int] = mapped_column(Integer, default=0)
    last_notified_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    next_escalation_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    snoozed_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    snoozed_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    snoozed_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class AlertNotificationStatus(str, Enum):
    QUEUED = "queued"
    SENT = "sent"
    FAILED = "failed"


class AlertNotification(Base):
    __tablename__ = "alert_notifications"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    alert_id: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[AlertSeverity] = mapped_column(SAEnum(AlertSeverity), index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)  # created | escalated | reminder
    channel_type: Mapped[str] = mapped_column(String(32))  # email | slack | webhook
    channel_target: Mapped[str] = mapped_column(String(255))
    recipient: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[AlertNotificationStatus] = mapped_column(SAEnum(AlertNotificationStatus), default=AlertNotificationStatus.QUEUED, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
