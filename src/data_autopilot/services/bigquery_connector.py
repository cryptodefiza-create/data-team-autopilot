from __future__ import annotations

import logging
from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any

from data_autopilot.config.settings import get_settings
from data_autopilot.services.cache_service import CacheService

logger = logging.getLogger(__name__)


@dataclass
class DryRunResult:
    total_bytes_processed: int
    estimated_cost_usd: float


class BigQueryConnector:
    """BigQuery connector with live and mock execution modes."""

    def __init__(self, cache: CacheService | None = None) -> None:
        self.cache = cache or CacheService()
        self.settings = get_settings()
        self._client_cache: dict[str, Any] = {}

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
        resolved = self._resolve_service_account(service_account_json)
        if not resolved:
            raise RuntimeError(
                "No BigQuery credentials available. "
                "Set BIGQUERY_SERVICE_ACCOUNT_JSON or connect a tenant service account."
            )

        cache_key = hashlib.sha256(
            json.dumps(resolved, sort_keys=True).encode()
        ).hexdigest()

        if cache_key in self._client_cache:
            return self._client_cache[cache_key]

        try:
            from google.cloud import bigquery  # type: ignore
            from google.oauth2 import service_account  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "BigQuery live mode requires google-cloud-bigquery and google-auth"
            ) from exc

        credentials = service_account.Credentials.from_service_account_info(resolved)
        client = bigquery.Client(
            project=self.settings.bigquery_project_id,
            credentials=credentials,
            location=self.settings.bigquery_location,
        )
        self._client_cache[cache_key] = client
        return client

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
                                    {"name": "email", "type": "STRING"},
                                    {"name": "created_at", "type": "TIMESTAMP"},
                                    {"name": "channel", "type": "STRING"},
                                    {"name": "country", "type": "STRING"},
                                ],
                                "partition_key": "created_at",
                                "row_count_est": 1_200,
                                "bytes_est": 5_000_000,
                                "freshness_hours": 2,
                            },
                            "events": {
                                "columns": [
                                    {"name": "event_id", "type": "STRING"},
                                    {"name": "user_id", "type": "STRING"},
                                    {"name": "event_name", "type": "STRING"},
                                    {"name": "timestamp", "type": "TIMESTAMP"},
                                    {"name": "properties", "type": "JSON"},
                                ],
                                "partition_key": "timestamp",
                                "row_count_est": 12_000,
                                "bytes_est": 900_000_000,
                                "freshness_hours": 2,
                            },
                            "orders": {
                                "columns": [
                                    {"name": "order_id", "type": "STRING"},
                                    {"name": "user_id", "type": "STRING"},
                                    {"name": "amount", "type": "FLOAT"},
                                    {"name": "status", "type": "STRING"},
                                    {"name": "created_at", "type": "TIMESTAMP"},
                                ],
                                "partition_key": "created_at",
                                "row_count_est": 5_500,
                                "bytes_est": 120_000_000,
                                "freshness_hours": 3,
                            },
                            "config": {
                                "columns": [
                                    {"name": "key", "type": "STRING"},
                                    {"name": "value", "type": "STRING"},
                                ],
                                "partition_key": None,
                                "row_count_est": 25,
                                "bytes_est": 4_000,
                                "freshness_hours": 720,
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
            actual_bytes = len(sql) * 2048

            if "COUNT(DISTINCT USER_ID) AS DAU" in sql_upper and "GROUP BY" in sql_upper:
                rows = []
                for i in range(14):
                    if i == 9:
                        continue  # missing data day
                    day = f"2026-02-{3 + i:02d}"
                    noise = ((i * 7 + 13) % 41) - 20  # deterministic ±20
                    weekday_offset = (3 + i) % 7  # 0=Mon for Feb 3 2026 (Tuesday)
                    wave = math.sin(weekday_offset * math.pi / 3.5) * 50
                    dau = int(300 + wave + noise)
                    dau = max(200, min(400, dau))
                    rows.append({"day": day, "dau": dau})
                return {"rows": rows, "actual_bytes": actual_bytes}

            if "SUM(AMOUNT)" in sql_upper and "GROUP BY" in sql_upper:
                rows = []
                for i in range(14):
                    if i == 9:
                        continue  # missing data day
                    day = f"2026-02-{3 + i:02d}"
                    weekday_idx = (1 + i) % 7  # Feb 3 2026 is Tuesday (idx 1)
                    is_weekend = weekday_idx >= 5
                    base = 3200.0 if is_weekend else 6500.0
                    noise = ((i * 11 + 7) % 81) * 10 - 400  # deterministic ±400
                    revenue = round(base + noise, 2)
                    rows.append({"day": day, "revenue": revenue})
                return {"rows": rows, "actual_bytes": actual_bytes}

            scalar_alias = re.search(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
            alias = scalar_alias.group(1) if scalar_alias else "value"
            seed = int(hashlib.sha256(sql.encode("utf-8")).hexdigest()[:8], 16)

            if "COUNT(DISTINCT USER_ID)" in sql_upper:
                value = 200 + (seed % 201)  # 200-400
                return {"rows": [{alias: value}], "actual_bytes": actual_bytes}
            if "SUM(AMOUNT)" in sql_upper:
                value = float(50_000 + (seed % 50_000))
                return {"rows": [{alias: value}], "actual_bytes": actual_bytes}
            return {"rows": [{"value": 1}], "actual_bytes": actual_bytes}

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

    # ── Autopilot ingestion methods ──────────────────────────────────

    def ensure_dataset(self, dataset_id: str = "autopilot") -> dict[str, Any]:
        if self.settings.bigquery_mock_mode:
            return {"dataset_id": dataset_id, "mode": "mock", "created": True}

        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("BigQuery live mode requires google-cloud-bigquery") from exc

        client = self._build_client()
        dataset_ref = f"{self.settings.bigquery_project_id}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.settings.bigquery_location
        dataset = client.create_dataset(dataset, exists_ok=True)
        return {"dataset_id": dataset_id, "mode": "live", "created": True}

    @staticmethod
    def _infer_schema(records: list[dict[str, Any]]) -> list[dict[str, str]]:
        type_map: dict[str, set[str]] = {}
        for row in records:
            for key, val in row.items():
                if val is None:
                    continue
                if isinstance(val, bool):
                    py_type = "bool"
                elif isinstance(val, int):
                    py_type = "int"
                elif isinstance(val, float):
                    py_type = "float"
                elif isinstance(val, (dict, list)):
                    py_type = "complex"
                else:
                    py_type = "str"
                type_map.setdefault(key, set()).add(py_type)

        bq_type_priority = {"str": "STRING", "complex": "STRING", "float": "FLOAT", "int": "INTEGER", "bool": "BOOLEAN"}

        schema: list[dict[str, str]] = []
        for col, types in type_map.items():
            if "str" in types or "complex" in types:
                bq_type = "STRING"
            elif "float" in types:
                bq_type = "FLOAT"
            elif "int" in types:
                bq_type = "INTEGER"
            elif "bool" in types:
                bq_type = "BOOLEAN"
            else:
                bq_type = "STRING"
            schema.append({"name": col, "type": bq_type})
        return schema

    @staticmethod
    def _serialize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        serialized = []
        for row in records:
            new_row: dict[str, Any] = {}
            for key, val in row.items():
                if isinstance(val, (dict, list)):
                    new_row[key] = json.dumps(val)
                else:
                    new_row[key] = val
            serialized.append(new_row)
        return serialized

    def create_table_from_records(
        self,
        records: list[dict[str, Any]],
        table_name: str,
        dataset_id: str = "autopilot",
    ) -> dict[str, Any]:
        if not records:
            return {"table": f"{dataset_id}.{table_name}", "rows_inserted": 0, "mode": "mock" if self.settings.bigquery_mock_mode else "live"}

        inferred = self._infer_schema(records)
        serialized = self._serialize_records(records)

        if self.settings.bigquery_mock_mode:
            logger.info("Mock: would create table %s.%s with %d rows", dataset_id, table_name, len(serialized))
            return {
                "table": f"{dataset_id}.{table_name}",
                "columns": inferred,
                "rows_inserted": len(serialized),
                "mode": "mock",
            }

        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("BigQuery live mode requires google-cloud-bigquery") from exc

        self.ensure_dataset(dataset_id)
        client = self._build_client()

        bq_schema = [
            bigquery.SchemaField(col["name"], col["type"], mode="NULLABLE")
            for col in inferred
        ]

        table_ref = f"{self.settings.bigquery_project_id}.{dataset_id}.{table_name}"
        table = bigquery.Table(table_ref, schema=bq_schema)
        table = client.create_table(table, exists_ok=True)

        errors = client.insert_rows_json(table_ref, serialized)
        if errors:
            logger.error("BigQuery insert errors: %s", errors)
            raise RuntimeError(f"BigQuery insert failed: {errors}")

        return {
            "table": f"{dataset_id}.{table_name}",
            "table_fq": table_ref,
            "columns": inferred,
            "rows_inserted": len(serialized),
            "mode": "live",
        }
