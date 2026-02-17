from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import Feedback, FeedbackType
from data_autopilot.schemas.common import FeedbackRequest


class FeedbackService:
    def create(self, db: Session, req: FeedbackRequest) -> Feedback:
        feedback = Feedback(
            id=f"fb_{uuid4().hex[:12]}",
            tenant_id=req.tenant_id,
            user_id=req.user_id,
            artifact_id=req.artifact_id,
            artifact_version=req.artifact_version,
            artifact_type=req.artifact_type,
            feedback_type=FeedbackType(req.feedback_type),
            comment=req.comment,
            prompt_hash=req.prompt_hash,
            tool_inputs_hash=req.tool_inputs_hash,
            session_id=req.session_id,
            provider=req.provider,
            model=req.model,
            was_fallback=req.was_fallback,
            conversation_context=json.dumps(req.conversation_context) if req.conversation_context else None,
            channel=req.channel,
            created_at=datetime.utcnow(),
        )
        db.add(feedback)
        db.commit()
        db.refresh(feedback)
        return feedback

    def summary(self, db: Session, tenant_id: str) -> dict:
        by_artifact = (
            db.execute(
                select(Feedback.artifact_type, Feedback.feedback_type, func.count(Feedback.id))
                .where(Feedback.tenant_id == tenant_id)
                .group_by(Feedback.artifact_type, Feedback.feedback_type)
            )
            .all()
        )

        negative_by_prompt = (
            db.execute(
                select(Feedback.prompt_hash, func.count(Feedback.id))
                .where(
                    Feedback.tenant_id == tenant_id,
                    Feedback.feedback_type == FeedbackType.NEGATIVE,
                    Feedback.prompt_hash.is_not(None),
                )
                .group_by(Feedback.prompt_hash)
                .order_by(func.count(Feedback.id).desc())
            )
            .all()
        )

        artifact_summary: dict[str, dict[str, int]] = {}
        for artifact_type, feedback_type, count in by_artifact:
            art_key = artifact_type.value if hasattr(artifact_type, "value") else str(artifact_type)
            fb_key = feedback_type.value if hasattr(feedback_type, "value") else str(feedback_type)
            artifact_summary.setdefault(art_key, {"positive": 0, "negative": 0})
            artifact_summary[art_key][fb_key] = int(count)

        return {
            "tenant_id": tenant_id,
            "artifact_feedback": artifact_summary,
            "negative_prompt_hashes": [
                {"prompt_hash": prompt_hash, "count": int(count)} for prompt_hash, count in negative_by_prompt
            ],
        }

    def list_for_review(self, db: Session, tenant_id: str, status: str | None = None, provider: str | None = None) -> list[dict]:
        stmt = select(Feedback).where(Feedback.tenant_id == tenant_id)
        if status == "resolved":
            stmt = stmt.where(Feedback.resolved.is_(True))
        elif status == "unresolved":
            stmt = stmt.where(Feedback.resolved.is_(False))
        if provider:
            stmt = stmt.where(Feedback.provider == provider)
        stmt = stmt.order_by(Feedback.created_at.desc())
        rows = db.execute(stmt).scalars().all()
        return [
            {
                "id": r.id,
                "user_id": r.user_id,
                "artifact_id": r.artifact_id,
                "artifact_type": r.artifact_type,
                "feedback_type": r.feedback_type.value if hasattr(r.feedback_type, "value") else str(r.feedback_type),
                "comment": r.comment,
                "provider": r.provider,
                "model": r.model,
                "channel": r.channel,
                "resolved": r.resolved,
                "resolved_by": r.resolved_by,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]

    def resolve(self, db: Session, feedback_id: str, resolved_by: str) -> Feedback | None:
        row = db.execute(select(Feedback).where(Feedback.id == feedback_id)).scalar_one_or_none()
        if not row:
            return None
        row.resolved = True
        row.resolved_at = datetime.utcnow()
        row.resolved_by = resolved_by
        db.commit()
        db.refresh(row)
        return row

    def provider_summary(self, db: Session, tenant_id: str) -> dict:
        by_provider_rows = (
            db.execute(
                select(Feedback.provider, Feedback.feedback_type, func.count(Feedback.id))
                .where(Feedback.tenant_id == tenant_id, Feedback.provider.is_not(None))
                .group_by(Feedback.provider, Feedback.feedback_type)
            )
            .all()
        )
        by_task_rows = (
            db.execute(
                select(Feedback.artifact_type, Feedback.feedback_type, func.count(Feedback.id))
                .where(Feedback.tenant_id == tenant_id)
                .group_by(Feedback.artifact_type, Feedback.feedback_type)
            )
            .all()
        )

        def _build_summary(rows: list) -> dict:
            buckets: dict[str, dict[str, int]] = {}
            for key, fb_type, count in rows:
                k = fb_type.value if hasattr(fb_type, "value") else str(fb_type)
                label = str(key) if key else "unknown"
                buckets.setdefault(label, {"positive": 0, "negative": 0})
                buckets[label][k] = int(count)
            for label, counts in buckets.items():
                total = counts["positive"] + counts["negative"]
                counts["satisfaction_rate"] = round(counts["positive"] / total, 4) if total > 0 else 0.0
            return buckets

        return {
            "by_provider": _build_summary(by_provider_rows),
            "by_task": _build_summary(by_task_rows),
        }
