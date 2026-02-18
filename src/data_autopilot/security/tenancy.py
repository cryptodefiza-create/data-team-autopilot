from __future__ import annotations

from fastapi import Header, HTTPException


def tenant_from_headers(x_tenant_id: str | None = Header(default=None)) -> str:
    if not x_tenant_id:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-Id header")
    return x_tenant_id


def ensure_tenant_scope(header_tenant_id: str, requested_org_id: str) -> None:
    if header_tenant_id != requested_org_id:
        raise HTTPException(
            status_code=403,
            detail="Tenant boundary violation: header tenant does not match requested org",
        )
