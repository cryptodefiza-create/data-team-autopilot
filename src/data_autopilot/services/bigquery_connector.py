from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data_autopilot.config.settings import get_settings
from data_autopilot.services.cache_service import CacheService


@dataclass
class DryRunResult:
    total_bytes_processed: int
    estimated_cost_usd: float


class BigQueryConnector:
    """BigQuery connector with live and mock execution modes."""

    def __init__(self, cache: CacheService | None = None) -> None:
        self.cache = cache or CacheService()
        self.settings = get_settings()

    def test_connection(self, service_account_json: dict | None = None) -> dict:
        if self.settings.bigquery_mock_mode:
            return {"ok": True, "mode": "mock"}

        try:
            from google.cloud import bigquery  # type: ignore
            from google.oauth2 import service_account  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "BigQuery live mode requires google-cloud-bigquery and google-auth"
            ) from exc

        credentials = None
        if service_account_json:
            credentials = service_account.Credentials.from_service_account_info(service_account_json)

        client = bigquery.Client(
            project=self.settings.bigquery_project_id,
            credentials=credentials,
            location=self.settings.bigquery_location,
        )
        query_job = client.query("SELECT 1 AS ok")
        rows = list(query_job.result(timeout=30))
        return {"ok": bool(rows and rows[0]["ok"] == 1), "mode": "live"}

    def introspect(self, connection_id: str) -> dict[str, Any]:
        key = f"schema:{connection_id}"
        cached = self.cache.get(key)
        if cached.cache_hit:
            data = dict(cached.value or {})
            data["cache_hit"] = True
            return data

        # Mock introspection payload. Live introspection can replace this section.
        schema = {
            "datasets": {
                "analytics": {
                    "tables": {
                        "users": {
                            "columns": [
                                {"name": "user_id", "type": "STRING"},
                                {"name": "created_at", "type": "TIMESTAMP"},
                                {"name": "email", "type": "STRING"},
                            ],
                            "partition_key": "created_at",
                        },
                        "events": {
                            "columns": [
                                {"name": "user_id", "type": "STRING"},
                                {"name": "created_at", "type": "TIMESTAMP"},
                                {"name": "event_name", "type": "STRING"},
                            ],
                            "partition_key": "created_at",
                        },
                        "orders": {
                            "columns": [
                                {"name": "order_id", "type": "STRING"},
                                {"name": "created_at", "type": "TIMESTAMP"},
                                {"name": "amount", "type": "FLOAT"},
                            ],
                            "partition_key": "created_at",
                        },
                    }
                }
            },
            "cache_hit": False,
        }
        self.cache.set(key, schema, ttl_seconds=self.settings.schema_cache_ttl_seconds)
        return schema

    def dry_run(self, sql: str) -> DryRunResult:
        estimated_bytes = max(1024, len(sql) * 2048)
        estimated_cost = (estimated_bytes / (1024**4)) * 5.0
        return DryRunResult(total_bytes_processed=estimated_bytes, estimated_cost_usd=estimated_cost)

    def execute_query(self, sql: str, timeout_seconds: int = 120) -> dict[str, Any]:
        # Mock execution for local/test mode.
        if self.settings.bigquery_mock_mode:
            if "COUNT(DISTINCT user_id) AS dau" in sql:
                return {"rows": [{"day": "2026-02-14", "dau": 12450}], "actual_bytes": len(sql) * 2048}
            if "SUM(amount)" in sql:
                return {"rows": [{"day": "2026-02-14", "revenue": 91234.11}], "actual_bytes": len(sql) * 2048}
            return {"rows": [{"value": 1}], "actual_bytes": len(sql) * 2048}

        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("BigQuery live mode requires google-cloud-bigquery") from exc

        client = bigquery.Client(project=self.settings.bigquery_project_id, location=self.settings.bigquery_location)
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        job = client.query(sql, job_config=job_config)
        rows = [dict(r.items()) for r in job.result(timeout=timeout_seconds)]
        processed = int(getattr(job, "total_bytes_processed", 0) or 0)
        return {"rows": rows, "actual_bytes": processed}
