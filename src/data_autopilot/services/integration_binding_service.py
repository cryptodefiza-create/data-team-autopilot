from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import IntegrationBinding, IntegrationBindingType


class IntegrationBindingService:
    def upsert(
        self,
        db: Session,
        tenant_id: str,
        binding_type: IntegrationBindingType,
        external_id: str,
    ) -> IntegrationBinding:
        row = db.execute(
            select(IntegrationBinding).where(
                IntegrationBinding.tenant_id == tenant_id,
                IntegrationBinding.binding_type == binding_type,
                IntegrationBinding.external_id == external_id,
            )
        ).scalar_one_or_none()
        if row is not None:
            return row
        row = IntegrationBinding(
            tenant_id=tenant_id,
            binding_type=binding_type,
            external_id=external_id,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def list_for_tenant(self, db: Session, tenant_id: str) -> list[IntegrationBinding]:
        return list(
            db.execute(
                select(IntegrationBinding)
                .where(IntegrationBinding.tenant_id == tenant_id)
                .order_by(IntegrationBinding.id.asc())
            ).scalars().all()
        )

    def delete(self, db: Session, tenant_id: str, binding_id: int) -> bool:
        row = db.execute(
            select(IntegrationBinding).where(
                IntegrationBinding.id == binding_id,
                IntegrationBinding.tenant_id == tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        db.execute(delete(IntegrationBinding).where(IntegrationBinding.id == row.id))
        db.commit()
        return True

    def resolve_for_slack(
        self,
        db: Session,
        team_id: str,
        user_id: str,
        requested_org: str | None,
        default_org: str,
    ) -> str | None:
        team_binding = self._get_by_external_id(db, IntegrationBindingType.SLACK_TEAM, team_id)
        user_binding = self._get_by_external_id(db, IntegrationBindingType.SLACK_USER, user_id)
        if requested_org:
            if team_binding is not None and team_binding.tenant_id == requested_org:
                return requested_org
            if user_binding is not None and user_binding.tenant_id == requested_org:
                return requested_org
            return None
        if team_binding is not None:
            return team_binding.tenant_id
        if user_binding is not None:
            return user_binding.tenant_id
        return default_org or None

    def resolve_for_telegram(
        self,
        db: Session,
        chat_id: str,
        user_id: str,
        requested_org: str | None,
        default_org: str,
    ) -> str | None:
        chat_binding = self._get_by_external_id(db, IntegrationBindingType.TELEGRAM_CHAT, chat_id)
        user_binding = self._get_by_external_id(db, IntegrationBindingType.TELEGRAM_USER, user_id)
        if requested_org:
            if chat_binding is not None and chat_binding.tenant_id == requested_org:
                return requested_org
            if user_binding is not None and user_binding.tenant_id == requested_org:
                return requested_org
            return None
        if chat_binding is not None:
            return chat_binding.tenant_id
        if user_binding is not None:
            return user_binding.tenant_id
        return default_org or None

    @staticmethod
    def _get_by_external_id(
        db: Session,
        binding_type: IntegrationBindingType,
        external_id: str,
    ) -> IntegrationBinding | None:
        if not external_id:
            return None
        return db.execute(
            select(IntegrationBinding).where(
                IntegrationBinding.binding_type == binding_type,
                IntegrationBinding.external_id == external_id,
            )
        ).scalar_one_or_none()
