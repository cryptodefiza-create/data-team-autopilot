from datetime import datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import ArtifactType, Feedback, FeedbackType
from data_autopilot.schemas.common import FeedbackRequest


class FeedbackService:
    def create(self, db: Session, req: FeedbackRequest) -> Feedback:
        feedback = Feedback(
            id=f"fb_{uuid4().hex[:12]}",
            tenant_id=req.tenant_id,
            user_id=req.user_id,
            artifact_id=req.artifact_id,
            artifact_version=req.artifact_version,
            artifact_type=ArtifactType(req.artifact_type),
            feedback_type=FeedbackType(req.feedback_type),
            comment=req.comment,
            prompt_hash=req.prompt_hash,
            tool_inputs_hash=req.tool_inputs_hash,
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
