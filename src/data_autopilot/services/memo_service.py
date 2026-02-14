from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import ArtifactType, CatalogTable
from data_autopilot.services.artifact_service import ArtifactService


@dataclass
class ValidationResult:
    passed: bool
    warnings: list[str]
    errors: list[str]


class MemoService:
    def __init__(self) -> None:
        self.artifacts = ArtifactService()

    def _packet(self, db: Session, tenant_id: str, timezone: str = "America/New_York") -> dict:
        end = datetime.utcnow().date() - timedelta(days=1)
        start = end - timedelta(days=6)
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=6)
        anomaly_notes: list[str] = []

        tables = db.execute(select(CatalogTable).where(CatalogTable.tenant_id == tenant_id)).scalars().all()
        for table in tables:
            if int(table.freshness_hours or 0) >= 6:
                anomaly_notes.append(
                    f"{table.dataset}.{table.table_name} table had {table.freshness_hours}-hour delay"
                )
            if int(table.row_count_est or 0) == 0:
                anomaly_notes.append(f"{table.dataset}.{table.table_name} has zero rows")

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "time_window": {
                "current": {"start": str(start), "end": str(end)},
                "previous": {"start": str(prev_start), "end": str(prev_end)},
                "timezone": timezone,
            },
            "kpis": [
                {
                    "metric_name": "DAU",
                    "current_value": 12450,
                    "previous_value": 11200,
                    "delta_absolute": 1250,
                    "delta_percent": 11.16,
                    "significance": "notable",
                    "query_hash": "q_dau",
                }
            ],
            "top_segments": [
                {
                    "parent_metric": "DAU",
                    "dimension": "channel",
                    "segment": "organic_search",
                    "current_value": 5200,
                    "previous_value": 3800,
                    "delta_contribution_pct": 45.2,
                }
            ],
            "anomaly_notes": anomaly_notes,
        }

    def _generate_memo(self, packet: dict) -> dict:
        dau = packet["kpis"][0]
        return {
            "headline_summary": ["DAU up 11.16% week over week."],
            "key_changes": [
                {
                    "metric_name": "DAU",
                    "current": dau["current_value"],
                    "previous": dau["previous_value"],
                    "delta_pct": dau["delta_percent"],
                    "interpretation": "Usage increased steadily.",
                    "confidence": "high",
                    "supporting_query_hashes": [dau["query_hash"]],
                }
            ],
            "likely_causes": [
                {
                    "hypothesis": "Growth may be from channel mix changes in organic search.",
                    "supporting_evidence": "DAU organic_search contribution 45.2%",
                    "evidence_type": "data_supported",
                }
            ],
            "recommended_actions": ["Review top acquisition channels for sustained growth drivers."],
            "data_quality_notes": packet["anomaly_notes"],
        }

    def validate(self, packet: dict, memo: dict) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        kpi_map = {k["metric_name"]: k for k in packet.get("kpis", [])}
        segment_names = {str(s.get("segment", "")).lower() for s in packet.get("top_segments", [])}
        packet_values = set()
        for kpi in kpi_map.values():
            packet_values.add(kpi["current_value"])
            packet_values.add(kpi["previous_value"])
            packet_values.add(kpi["delta_percent"])

        for change in memo.get("key_changes", []):
            metric = change.get("metric_name")
            if metric not in kpi_map:
                errors.append(f"Unknown metric: {metric}")
                continue
            if change.get("current") not in packet_values:
                errors.append(f"Current value mismatch for {metric}")
            if change.get("previous") not in packet_values:
                errors.append(f"Previous value mismatch for {metric}")

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
        packet_hash = str(abs(hash(str(packet))))
        artifact = self.artifacts.create_or_update(
            db,
            tenant_id=tenant_id,
            artifact_type=ArtifactType.MEMO,
            data={"packet": packet, "memo": memo, "packet_hash": packet_hash, "validation": validation.__dict__},
            query_hashes=query_hashes,
        )
        return {"artifact_id": artifact.id, "version": artifact.version, "validation": validation.__dict__}
