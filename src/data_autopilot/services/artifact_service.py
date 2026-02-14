from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import Artifact, ArtifactType, ArtifactVersion, CatalogTable


class ArtifactService:
    def get_latest(self, db: Session, tenant_id: str, artifact_type: ArtifactType) -> Artifact | None:
        return (
            db.execute(
                select(Artifact)
                .where(Artifact.tenant_id == tenant_id, Artifact.type == artifact_type)
                .order_by(Artifact.created_at.desc())
            )
            .scalars()
            .first()
        )

    def create_or_update(
        self,
        db: Session,
        tenant_id: str,
        artifact_type: ArtifactType,
        data: dict,
        query_hashes: list[str],
    ) -> Artifact:
        existing = self.get_latest(db, tenant_id=tenant_id, artifact_type=artifact_type)
        if existing is None:
            artifact = Artifact(
                id=f"art_{uuid4().hex[:12]}",
                tenant_id=tenant_id,
                type=artifact_type,
                version=1,
                data=deepcopy(data),
                query_hashes=list(query_hashes),
                created_at=datetime.utcnow(),
            )
            db.add(artifact)
            db.commit()
            db.refresh(artifact)
            self._snapshot(db, artifact)
            return artifact

        existing.version += 1
        existing.data = deepcopy(data)
        existing.query_hashes = list(query_hashes)
        db.add(existing)
        db.commit()
        db.refresh(existing)
        self._snapshot(db, existing)
        return existing

    def _snapshot(self, db: Session, artifact: Artifact) -> None:
        snap = ArtifactVersion(
            artifact_id=artifact.id,
            tenant_id=artifact.tenant_id,
            version=artifact.version,
            data=deepcopy(artifact.data),
            query_hashes=list(artifact.query_hashes or []),
        )
        db.add(snap)
        db.commit()

    def list_for_tenant(self, db: Session, tenant_id: str, artifact_type: ArtifactType | None = None) -> list[Artifact]:
        stmt = select(Artifact).where(Artifact.tenant_id == tenant_id).order_by(Artifact.created_at.desc())
        if artifact_type is not None:
            stmt = stmt.where(Artifact.type == artifact_type)
        return list(db.execute(stmt).scalars().all())

    def get(self, db: Session, artifact_id: str, tenant_id: str) -> Artifact | None:
        return (
            db.execute(
                select(Artifact).where(Artifact.id == artifact_id, Artifact.tenant_id == tenant_id)
            )
            .scalars()
            .first()
        )

    def versions(self, db: Session, artifact_id: str, tenant_id: str) -> list[ArtifactVersion]:
        stmt = (
            select(ArtifactVersion)
            .where(ArtifactVersion.artifact_id == artifact_id, ArtifactVersion.tenant_id == tenant_id)
            .order_by(ArtifactVersion.version.asc())
        )
        return list(db.execute(stmt).scalars().all())

    def memo_wow(self, db: Session, artifact_id: str, tenant_id: str) -> dict:
        versions = self.versions(db, artifact_id=artifact_id, tenant_id=tenant_id)
        if len(versions) < 2:
            return {"artifact_id": artifact_id, "tenant_id": tenant_id, "rows": [], "note": "Need at least 2 versions"}

        prev = versions[-2].data.get("packet", {})
        curr = versions[-1].data.get("packet", {})

        prev_map = {k["metric_name"]: k for k in prev.get("kpis", [])}
        curr_map = {k["metric_name"]: k for k in curr.get("kpis", [])}

        rows = []
        for metric_name in sorted(set(prev_map.keys()) | set(curr_map.keys())):
            p = prev_map.get(metric_name)
            c = curr_map.get(metric_name)
            p_val = p.get("current_value") if p else None
            c_val = c.get("current_value") if c else None
            delta = None
            if p_val is not None and c_val is not None:
                delta = c_val - p_val
            rows.append(
                {
                    "metric": metric_name,
                    "previous_week_value": p_val,
                    "current_week_value": c_val,
                    "change": delta,
                }
            )

        return {
            "artifact_id": artifact_id,
            "tenant_id": tenant_id,
            "from_version": versions[-2].version,
            "to_version": versions[-1].version,
            "rows": rows,
        }

    def lineage(self, db: Session, artifact_id: str, tenant_id: str) -> dict:
        artifact = self.get(db, artifact_id=artifact_id, tenant_id=tenant_id)
        if artifact is None:
            return {"artifact_id": artifact_id, "tenant_id": tenant_id, "nodes": [], "edges": []}

        tables = db.execute(select(CatalogTable).where(CatalogTable.tenant_id == tenant_id)).scalars().all()
        root_id = f"artifact:{artifact.id}"
        nodes = [
            {
                "id": root_id,
                "kind": "artifact",
                "label": f"{artifact.type.value} v{artifact.version}",
                "meta": {"artifact_id": artifact.id, "query_hashes": artifact.query_hashes},
            }
        ]
        edges = []
        for table in tables:
            tid = f"table:{table.dataset}.{table.table_name}"
            nodes.append(
                {
                    "id": tid,
                    "kind": "table",
                    "label": f"{table.dataset}.{table.table_name}",
                    "meta": {"row_count_est": table.row_count_est, "freshness_hours": table.freshness_hours},
                }
            )
            edges.append({"from": root_id, "to": tid, "type": "depends_on"})

        return {"artifact_id": artifact_id, "tenant_id": tenant_id, "nodes": nodes, "edges": edges}

    def diff(self, db: Session, artifact_id: str, tenant_id: str, from_version: int | None = None, to_version: int | None = None) -> dict:
        versions = self.versions(db, artifact_id=artifact_id, tenant_id=tenant_id)
        if len(versions) < 2:
            return {"artifact_id": artifact_id, "tenant_id": tenant_id, "changes": [], "note": "Need at least 2 versions"}

        if from_version is None or to_version is None:
            a = versions[-2]
            b = versions[-1]
        else:
            amap = {v.version: v for v in versions}
            if from_version not in amap or to_version not in amap:
                return {"artifact_id": artifact_id, "tenant_id": tenant_id, "changes": [], "note": "Version not found"}
            a = amap[from_version]
            b = amap[to_version]

        set_a = set(a.query_hashes or [])
        set_b = set(b.query_hashes or [])
        added_queries = sorted(set_b - set_a)
        removed_queries = sorted(set_a - set_b)
        data_changed = a.data != b.data

        changes = []
        if added_queries:
            changes.append({"type": "query_hash_added", "values": added_queries})
        if removed_queries:
            changes.append({"type": "query_hash_removed", "values": removed_queries})
        if data_changed:
            changes.append({"type": "artifact_data_changed", "from_version": a.version, "to_version": b.version})

        return {
            "artifact_id": artifact_id,
            "tenant_id": tenant_id,
            "from_version": a.version,
            "to_version": b.version,
            "changes": changes,
        }
