from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import Connection
from data_autopilot.services.secrets_manager import SecretsManager


def load_active_connection_credentials(db: Session, tenant_id: str) -> tuple[str | None, dict | None]:
    row = db.execute(
        select(Connection)
        .where(Connection.tenant_id == tenant_id, Connection.status == "active")
        .order_by(Connection.created_at.desc())
    ).scalar_one_or_none()
    if row is None:
        return None, None
    encrypted = dict(row.config_encrypted or {})
    if not encrypted:
        return row.id, None
    creds = SecretsManager().decrypt(encrypted)
    return row.id, creds
