from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from data_autopilot.config.settings import get_settings
from data_autopilot.api.routes import router
from data_autopilot.db.base import Base
from data_autopilot.db.session import SessionLocal, engine
from data_autopilot.services.audit import AuditService
from data_autopilot.services.runtime_checks import run_startup_checks

Base.metadata.create_all(bind=engine)


def _ensure_schema_compat() -> None:
    """Apply lightweight compatibility migrations for local SQLite dev DBs."""
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info('workflow_queue')")).fetchall()
        names = {str(r[1]) for r in rows}
        if "attempts" not in names:
            conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN attempts INTEGER DEFAULT 0"))
        if "error_history" not in names:
            conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN error_history JSON DEFAULT '[]'"))

        rows_alert = conn.execute(text("PRAGMA table_info('alerts')")).fetchall()
        if rows_alert:
            alert_names = {str(r[1]) for r in rows_alert}
            if "snoozed_until" not in alert_names:
                conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_until DATETIME"))
            if "snoozed_by" not in alert_names:
                conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_by VARCHAR(64)"))
            if "snoozed_reason" not in alert_names:
                conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_reason VARCHAR(255)"))

        rows_ntf = conn.execute(text("PRAGMA table_info('alert_notifications')")).fetchall()
        if rows_ntf:
            ntf_names = {str(r[1]) for r in rows_ntf}
            if "retry_count" not in ntf_names:
                conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN retry_count INTEGER DEFAULT 0"))
            if "next_retry_at" not in ntf_names:
                conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN next_retry_at DATETIME"))
            if "last_error" not in ntf_names:
                conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN last_error VARCHAR(255)"))

        rows_fb = conn.execute(text("PRAGMA table_info('feedback')")).fetchall()
        if rows_fb:
            fb_names = {str(r[1]) for r in rows_fb}
            for col, ddl in [
                ("session_id", "VARCHAR(128)"),
                ("provider", "VARCHAR(64)"),
                ("model", "VARCHAR(128)"),
                ("was_fallback", "BOOLEAN DEFAULT 0"),
                ("conversation_context", "TEXT"),
                ("channel", "VARCHAR(64)"),
                ("resolved", "BOOLEAN DEFAULT 0"),
                ("resolved_at", "DATETIME"),
                ("resolved_by", "VARCHAR(64)"),
            ]:
                if col not in fb_names:
                    conn.execute(text(f"ALTER TABLE feedback ADD COLUMN {col} {ddl}"))


_ensure_schema_compat()

@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    run_startup_checks(settings)
    yield


app = FastAPI(title="Data Team Autopilot", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(HTTPException)
async def audited_http_exception_handler(request: Request, exc: HTTPException):
    tenant_id = request.headers.get("X-Tenant-Id", "unknown")
    role = request.headers.get("X-User-Role", "unknown")
    audit = AuditService()
    db = SessionLocal()
    try:
        audit.log(
            db,
            tenant_id=tenant_id,
            event_type="http_exception",
            payload={
                "status_code": exc.status_code,
                "detail": str(exc.detail),
                "method": request.method,
                "path": request.url.path,
                "role": role,
            },
        )
    finally:
        db.close()

    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
