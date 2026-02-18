from __future__ import annotations

from sqlalchemy.orm import Session

from data_autopilot.models.entities import AlertSeverity
from data_autopilot.services.agent_service import AgentService
from data_autopilot.services.alert_service import AlertService
from data_autopilot.services.artifact_service import ArtifactService
from data_autopilot.services.audit import AuditService
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.channel_integrations import ChannelIntegrationsService
from data_autopilot.services.connector_service import ConnectorService
from data_autopilot.services.conversation_service import ConversationService
from data_autopilot.services.degradation_service import DegradationService
from data_autopilot.services.feedback_service import FeedbackService
from data_autopilot.services.integration_binding_service import IntegrationBindingService
from data_autopilot.services.metabase_client import MetabaseClient
from data_autopilot.services.notification_service import NotificationService
from data_autopilot.services.query_service import QueryService
from data_autopilot.services.tenant_admin_service import TenantAdminService
from data_autopilot.services.workflow_service import WorkflowService


agent_service = AgentService()
conversation_service = ConversationService()
feedback_service = FeedbackService()
workflow_service = WorkflowService()
connector_service = ConnectorService()
metabase_client = MetabaseClient()
bigquery_connector = BigQueryConnector()
degradation_service = DegradationService()
artifact_service = ArtifactService()
audit_service = AuditService()
query_service = QueryService()
alert_service = AlertService()
notification_service = NotificationService()
tenant_admin_service = TenantAdminService()
channel_integrations_service = ChannelIntegrationsService()
integration_binding_service = IntegrationBindingService()


def _build_mode1_fetcher():
    from data_autopilot.config.settings import get_settings
    from data_autopilot.services.mode1.live_fetcher import LiveFetcher
    from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
    from data_autopilot.services.mode1.request_parser import RequestParser
    from data_autopilot.services.providers.alchemy import AlchemyProvider
    from data_autopilot.services.providers.coingecko import CoinGeckoProvider
    from data_autopilot.services.providers.helius import HeliusProvider

    s = get_settings()
    providers = {
        "helius": HeliusProvider(api_key=s.helius_api_key),
        "alchemy": AlchemyProvider(api_key=s.alchemy_api_key),
        "coingecko": CoinGeckoProvider(),
    }
    key_mgr = PlatformKeyManager()
    if s.helius_api_key:
        key_mgr.register("helius", [s.helius_api_key])
    if s.alchemy_api_key:
        key_mgr.register("alchemy", [s.alchemy_api_key])
    parser = RequestParser()
    return LiveFetcher(
        providers=providers,
        key_manager=key_mgr,
        parser=parser,
        tier=s.blockchain_provider_tier,
    )


mode1_fetcher = _build_mode1_fetcher()


def auto_alert_from_workflow_result(db: Session, org_id: str, workflow_type: str, result: dict) -> None:
    if result.get("workflow_status") != "partial_failure":
        return
    failed = result.get("failed_step", {})
    failed_step = str(failed.get("step", "unknown_step"))
    message = str(failed.get("error", "workflow step failed"))
    alert_service.create_or_update(
        db,
        tenant_id=org_id,
        dedupe_key=f"workflow_partial_failure:{workflow_type}:{failed_step}",
        title=f"{workflow_type} workflow partial failure",
        message=f"Step '{failed_step}' failed: {message}",
        severity=AlertSeverity.P1,
        source_type="workflow",
        source_id=workflow_type,
    )


def auto_alert_from_memo_anomalies(db: Session, org_id: str, artifact_id: str) -> None:
    artifact = artifact_service.get(db, artifact_id=artifact_id, tenant_id=org_id)
    if artifact is None:
        return
    packet = (artifact.data or {}).get("packet", {})
    notes = packet.get("anomaly_notes", [])
    if not isinstance(notes, list) or not notes:
        return
    for note in notes:
        key = str(note).strip()
        if not key:
            continue
        alert_service.create_or_update(
            db,
            tenant_id=org_id,
            dedupe_key=f"memo_anomaly:{abs(hash(key))}",
            title="Data quality anomaly detected",
            message=key,
            severity=AlertSeverity.P2,
            source_type="data_quality",
            source_id="memo",
        )
