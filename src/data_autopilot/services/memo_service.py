from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import ArtifactType, CatalogTable
from data_autopilot.services.artifact_service import ArtifactService
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.connection_context import load_active_connection_credentials
from data_autopilot.services.llm_client import LLMClient


@dataclass
class ValidationResult:
    passed: bool
    warnings: list[str]
    errors: list[str]


class MemoService:
    def __init__(self) -> None:
        self.artifacts = ArtifactService()
        self.connector = BigQueryConnector()
        self.llm = LLMClient()

    @staticmethod
    def _delta(current: float, previous: float) -> tuple[float, float]:
        delta_absolute = current - previous
        if previous == 0:
            delta_percent = 100.0 if current > 0 else 0.0
        else:
            delta_percent = (delta_absolute / previous) * 100.0
        return delta_absolute, round(delta_percent, 2)

    def _query_scalar(self, sql: str, key: str, creds: dict | None) -> float:
        rows = self.connector.execute_query(sql, service_account_json=creds).get("rows", [])
        if not rows:
            return 0.0
        return float(rows[0].get(key, 0.0) or 0.0)

    def _packet(self, db: Session, tenant_id: str, timezone: str = "America/New_York") -> dict:
        end = datetime.utcnow().date() - timedelta(days=1)
        start = end - timedelta(days=6)
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=6)
        anomaly_notes: list[str] = []
        _connection_id, creds = load_active_connection_credentials(db, tenant_id=tenant_id)

        tables = db.execute(select(CatalogTable).where(CatalogTable.tenant_id == tenant_id)).scalars().all()
        for table in tables:
            if int(table.freshness_hours or 0) >= 6:
                anomaly_notes.append(
                    f"{table.dataset}.{table.table_name} table had {table.freshness_hours}-hour delay"
                )
            if int(table.row_count_est or 0) == 0:
                anomaly_notes.append(f"{table.dataset}.{table.table_name} has zero rows")

        kpis: list[dict] = []
        current_dau = self._query_scalar(
            (
                f"SELECT COUNT(DISTINCT user_id) AS value FROM analytics.events "
                f"WHERE DATE(created_at) >= DATE '{start}' AND DATE(created_at) <= DATE '{end}'"
            ),
            key="value",
            creds=creds,
        )
        previous_dau = self._query_scalar(
            (
                f"SELECT COUNT(DISTINCT user_id) AS value FROM analytics.events "
                f"WHERE DATE(created_at) >= DATE '{prev_start}' AND DATE(created_at) <= DATE '{prev_end}'"
            ),
            key="value",
            creds=creds,
        )
        dau_abs, dau_pct = self._delta(current_dau, previous_dau)
        kpis.append(
            {
                "metric_name": "DAU",
                "current_value": int(round(current_dau)),
                "previous_value": int(round(previous_dau)),
                "delta_absolute": int(round(dau_abs)),
                "delta_percent": dau_pct,
                "significance": "major" if abs(dau_pct) > 25 else ("notable" if abs(dau_pct) > 10 else "normal"),
                "query_hash": "q_dau",
            }
        )

        has_orders = any(t.table_name == "orders" for t in tables)
        if has_orders:
            current_rev = self._query_scalar(
                (
                    f"SELECT SUM(amount) AS value FROM analytics.orders "
                    f"WHERE DATE(created_at) >= DATE '{start}' AND DATE(created_at) <= DATE '{end}'"
                ),
                key="value",
                creds=creds,
            )
            previous_rev = self._query_scalar(
                (
                    f"SELECT SUM(amount) AS value FROM analytics.orders "
                    f"WHERE DATE(created_at) >= DATE '{prev_start}' AND DATE(created_at) <= DATE '{prev_end}'"
                ),
                key="value",
                creds=creds,
            )
            rev_abs, rev_pct = self._delta(current_rev, previous_rev)
            kpis.append(
                {
                    "metric_name": "Revenue",
                    "current_value": round(current_rev, 2),
                    "previous_value": round(previous_rev, 2),
                    "delta_absolute": round(rev_abs, 2),
                    "delta_percent": rev_pct,
                    "significance": "major" if abs(rev_pct) > 25 else ("notable" if abs(rev_pct) > 10 else "normal"),
                    "query_hash": "q_revenue",
                }
            )

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "time_window": {
                "current": {"start": str(start), "end": str(end)},
                "previous": {"start": str(prev_start), "end": str(prev_end)},
                "timezone": timezone,
            },
            "kpis": kpis,
            "top_segments": [],
            "anomaly_notes": anomaly_notes,
        }

    def _generate_memo_fallback(self, packet: dict) -> dict:
        kpis = list(packet.get("kpis", []))
        if not kpis:
            return {
                "headline_summary": ["No KPI data available for this period."],
                "key_changes": [],
                "likely_causes": [{"hypothesis": "Insufficient KPI data to infer causes.", "supporting_evidence": "no supporting data", "evidence_type": "speculative"}],
                "recommended_actions": ["Verify source data availability and rerun memo generation."],
                "data_quality_notes": packet.get("anomaly_notes", []),
            }
        lead = kpis[0]
        return {
            "headline_summary": [f"{lead['metric_name']} changed {lead['delta_percent']}% week over week."],
            "key_changes": [
                {
                    "metric_name": k["metric_name"],
                    "current": k["current_value"],
                    "previous": k["previous_value"],
                    "delta_pct": k["delta_percent"],
                    "interpretation": f"{k['metric_name']} moved by {k['delta_percent']}% compared to the previous period.",
                    "confidence": "high" if k.get("significance") in {"major", "critical"} else "medium",
                    "supporting_query_hashes": [k["query_hash"]],
                }
                for k in kpis
            ],
            "likely_causes": [
                {
                    "hypothesis": f"{lead['metric_name']} movement likely reflects recent demand and engagement shifts.",
                    "supporting_evidence": f"{lead['metric_name']} delta {lead['delta_percent']}%",
                    "evidence_type": "data_supported" if abs(float(lead.get("delta_percent", 0.0))) > 0 else "speculative",
                }
            ],
            "recommended_actions": ["Review top acquisition channels for sustained growth drivers."],
            "data_quality_notes": packet["anomaly_notes"],
        }

    def _generate_memo(self, packet: dict) -> dict:
        if not self.llm.is_configured():
            return self._generate_memo_fallback(packet)

        system_prompt = (
            "You are a data analyst writing a weekly executive memo. "
            "Return only JSON with keys: headline_summary, key_changes, likely_causes, recommended_actions, data_quality_notes. "
            "Use only values present in the packet. Never invent metrics."
        )
        user_prompt = (
            "Create weekly memo from this packet:\n"
            + json.dumps(packet, sort_keys=True)
        )
        try:
            memo = self.llm.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            if not isinstance(memo.get("headline_summary"), list):
                return self._generate_memo_fallback(packet)
            if not isinstance(memo.get("key_changes"), list):
                return self._generate_memo_fallback(packet)
            if not isinstance(memo.get("likely_causes"), list):
                return self._generate_memo_fallback(packet)
            if not isinstance(memo.get("recommended_actions"), list):
                return self._generate_memo_fallback(packet)
            if not isinstance(memo.get("data_quality_notes"), list):
                return self._generate_memo_fallback(packet)
            return memo
        except Exception:
            return self._generate_memo_fallback(packet)

    def validate(self, packet: dict, memo: dict) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        kpi_map = {k["metric_name"]: k for k in packet.get("kpis", [])}
        segment_names = {str(s.get("segment", "")).lower() for s in packet.get("top_segments", [])}

        for change in memo.get("key_changes", []):
            metric = change.get("metric_name")
            kpi = kpi_map.get(metric)
            if kpi is None:
                errors.append(f"Unknown metric: {metric}")
                continue
            if change.get("current") != kpi.get("current_value"):
                errors.append(f"Current value mismatch for {metric}")
            if change.get("previous") != kpi.get("previous_value"):
                errors.append(f"Previous value mismatch for {metric}")
            if change.get("delta_pct") != kpi.get("delta_percent"):
                errors.append(f"Delta percent mismatch for {metric}")

        notable = {k["metric_name"] for k in packet.get("kpis", []) if k.get("significance") in {"notable", "major", "critical"}}
        covered = {c.get("metric_name") for c in memo.get("key_changes", [])}
        missing = notable - covered
        if missing:
            warnings.append(f"Missing notable metrics in memo: {sorted(missing)}")

        for cause in memo.get("likely_causes", []):
            et = cause.get("evidence_type")
            if et not in {"data_supported", "speculative"}:
                errors.append("Invalid evidence_type")
            if et == "data_supported":
                text = str(cause.get("supporting_evidence", "")).lower()
                has_metric_ref = any(name.lower() in text for name in kpi_map.keys())
                has_segment_ref = any(seg and seg in text for seg in segment_names)
                if not has_metric_ref and not has_segment_ref:
                    warnings.append("Downgrading unsupported data_supported cause to speculative")
                    cause["evidence_type"] = "speculative"
                    cause["supporting_evidence"] = "no supporting data"

        return ValidationResult(passed=len(errors) == 0, warnings=warnings, errors=errors)

    def generate(self, db: Session, tenant_id: str) -> dict:
        packet = self._packet(db, tenant_id=tenant_id)

        attempts = 0
        memo = {}
        validation = ValidationResult(False, [], ["not started"])
        while attempts < 3:
            attempts += 1
            memo = self._generate_memo(packet)
            validation = self.validate(packet, memo)
            if validation.passed:
                break

        if not validation.passed:
            memo = {
                "headline_summary": ["Automated narrative unavailable; raw metrics attached."],
                "key_changes": packet["kpis"],
                "likely_causes": [],
                "recommended_actions": ["Review raw metrics and rerun memo generation."],
                "data_quality_notes": packet["anomaly_notes"],
                "fallback": True,
            }

        query_hashes = [k["query_hash"] for k in packet["kpis"]]
        packet_hash = hashlib.sha256(json.dumps(packet, sort_keys=True).encode("utf-8")).hexdigest()
        artifact = self.artifacts.create_or_update(
            db,
            tenant_id=tenant_id,
            artifact_type=ArtifactType.MEMO,
            data={"packet": packet, "memo": memo, "packet_hash": packet_hash, "validation": validation.__dict__},
            query_hashes=query_hashes,
        )
        return {"artifact_id": artifact.id, "version": artifact.version, "validation": validation.__dict__}
