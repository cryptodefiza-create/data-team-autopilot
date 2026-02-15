from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
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

    def _resolve_service_account(self, service_account_json: dict | None) -> dict | None:
        if service_account_json:
            return service_account_json
        raw = (self.settings.bigquery_service_account_json or "").strip()
        if not raw:
            return None
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError("BIGQUERY_SERVICE_ACCOUNT_JSON must be a JSON object")
        return parsed

    def _build_client(self, service_account_json: dict | None = None):
        try:
            from google.cloud import bigquery  # type: ignore
            from google.oauth2 import service_account  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "BigQuery live mode requires google-cloud-bigquery and google-auth"
            ) from exc

        resolved = self._resolve_service_account(service_account_json)
        credentials = None
        if resolved:
            credentials = service_account.Credentials.from_service_account_info(resolved)
        else:
            raise RuntimeError(
                "No BigQuery credentials available. Set BIGQUERY_SERVICE_ACCOUNT_JSON or connect a tenant service account."
            )
        return bigquery.Client(
            project=self.settings.bigquery_project_id,
            credentials=credentials,
            location=self.settings.bigquery_location,
        )

    def test_connection(self, service_account_json: dict | None = None) -> dict:
        if self.settings.bigquery_mock_mode:
            return {"ok": True, "mode": "mock"}
        client = self._build_client(service_account_json=service_account_json)
        query_job = client.query("SELECT 1 AS ok")
        rows = list(query_job.result(timeout=30))
        return {"ok": bool(rows and rows[0]["ok"] == 1), "mode": "live"}

    def introspect(self, connection_id: str, service_account_json: dict | None = None) -> dict[str, Any]:
        key = f"schema:{connection_id}"
        cached = self.cache.get(key)
        if cached.cache_hit:
            data = dict(cached.value or {})
            data["cache_hit"] = True
            return data

        if self.settings.bigquery_mock_mode:
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
                                "row_count_est": 50_000,
                                "bytes_est": 5_000_000,
                                "freshness_hours": 2,
                            },
                            "events": {
                                "columns": [
                                    {"name": "user_id", "type": "STRING"},
                                    {"name": "created_at", "type": "TIMESTAMP"},
                                    {"name": "event_name", "type": "STRING"},
                                ],
                                "partition_key": "created_at",
                                "row_count_est": 5_000_000,
                                "bytes_est": 900_000_000,
                                "freshness_hours": 2,
                            },
                            "orders": {
                                "columns": [
                                    {"name": "order_id", "type": "STRING"},
                                    {"name": "created_at", "type": "TIMESTAMP"},
                                    {"name": "amount", "type": "FLOAT"},
                                ],
                                "partition_key": "created_at",
                                "row_count_est": 220_000,
                                "bytes_est": 120_000_000,
                                "freshness_hours": 3,
                            },
                        }
                    }
                },
                "cache_hit": False,
            }
        else:
            client = self._build_client(service_account_json=service_account_json)
            datasets_payload: dict[str, dict[str, Any]] = {}
            for ds in client.list_datasets():
                ds_id = ds.dataset_id
                tables_payload: dict[str, Any] = {}
                for t in client.list_tables(ds.reference):
                    table_obj = client.get_table(t.reference)
                    partition_key = None
                    tp = getattr(table_obj, "time_partitioning", None)
                    if tp is not None:
                        partition_key = tp.field or "_PARTITIONTIME"
                    tables_payload[table_obj.table_id] = {
                        "columns": [{"name": c.name, "type": str(c.field_type)} for c in table_obj.schema],
                        "partition_key": partition_key,
                        "row_count_est": int(getattr(table_obj, "num_rows", 0) or 0),
                        "bytes_est": int(getattr(table_obj, "num_bytes", 0) or 0),
                        "freshness_hours": 0,
                    }
                datasets_payload[ds_id] = {"tables": tables_payload}
            schema = {"datasets": datasets_payload, "cache_hit": False}

        self.cache.set(key, schema, ttl_seconds=self.settings.schema_cache_ttl_seconds)
        return schema

    def dry_run(self, sql: str, service_account_json: dict | None = None) -> DryRunResult:
        if not self.settings.bigquery_mock_mode:
            try:
                from google.cloud import bigquery  # type: ignore
            except Exception as exc:  # pragma: no cover
                raise RuntimeError("BigQuery live mode requires google-cloud-bigquery") from exc
            client = self._build_client(service_account_json=service_account_json)
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = client.query(sql, job_config=job_config)
            estimated_bytes = int(getattr(job, "total_bytes_processed", 0) or 0)
            estimated_cost = (estimated_bytes / (1024**4)) * 5.0
            return DryRunResult(total_bytes_processed=estimated_bytes, estimated_cost_usd=estimated_cost)

        estimated_bytes = max(1024, len(sql) * 2048)
        estimated_cost = (estimated_bytes / (1024**4)) * 5.0
        return DryRunResult(total_bytes_processed=estimated_bytes, estimated_cost_usd=estimated_cost)

    def execute_query(self, sql: str, timeout_seconds: int = 120, service_account_json: dict | None = None) -> dict[str, Any]:
        if self.settings.bigquery_mock_mode:
            sql_upper = sql.upper()
            if "COUNT(DISTINCT USER_ID) AS DAU" in sql_upper and "GROUP BY" in sql_upper:
                return {"rows": [{"day": "2026-02-14", "dau": 12450}], "actual_bytes": len(sql) * 2048}
            if "SUM(AMOUNT)" in sql_upper and "GROUP BY" in sql_upper:
                return {"rows": [{"day": "2026-02-14", "revenue": 91234.11}], "actual_bytes": len(sql) * 2048}
            scalar_alias = re.search(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
            alias = scalar_alias.group(1) if scalar_alias else "value"
            seed = int(hashlib.sha256(sql.encode("utf-8")).hexdigest()[:8], 16)
            if "COUNT(DISTINCT USER_ID)" in sql_upper:
                value = 10_000 + (seed % 4_000)
                return {"rows": [{alias: value}], "actual_bytes": len(sql) * 2048}
            if "SUM(AMOUNT)" in sql_upper:
                value = float(50_000 + (seed % 50_000))
                return {"rows": [{alias: value}], "actual_bytes": len(sql) * 2048}
            return {"rows": [{"value": 1}], "actual_bytes": len(sql) * 2048}

        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("BigQuery live mode requires google-cloud-bigquery") from exc
        client = self._build_client(service_account_json=service_account_json)
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        job = client.query(sql, job_config=job_config)
        rows = [dict(r.items()) for r in job.result(timeout=timeout_seconds)]
        processed = int(getattr(job, "total_bytes_processed", 0) or 0)
        return {"rows": rows, "actual_bytes": processed}
