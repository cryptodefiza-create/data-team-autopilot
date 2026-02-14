from __future__ import annotations

from fastapi import Header, HTTPException

from data_autopilot.models.entities import Role


def ensure_can_run_queries(role: Role) -> None:
    if role == Role.VIEWER:
        raise HTTPException(status_code=403, detail="Viewer role cannot execute queries")


def role_from_headers(x_user_role: str | None = Header(default=None)) -> Role:
    if not x_user_role:
        raise HTTPException(status_code=400, detail="Missing X-User-Role header")
    try:
        return Role(x_user_role.lower())
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid X-User-Role header") from exc


def require_member_or_admin(role: Role) -> None:
    if role not in {Role.ADMIN, Role.MEMBER}:
        raise HTTPException(status_code=403, detail="Insufficient role: requires member or admin")


def require_admin(role: Role) -> None:
    if role != Role.ADMIN:
        raise HTTPException(status_code=403, detail="Insufficient role: requires admin")
