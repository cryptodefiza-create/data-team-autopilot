from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ColumnProfile, SchemaProfile, TableProfile

logger = logging.getLogger(__name__)


class PostgresReadConnector:
    """Read-only PostgreSQL connector for warehouse Mode 2.

    Mock mode uses an in-memory schema for testing.
    Production would use psycopg2 / asyncpg.
    """

    def __init__(self, connection_string: str = "", mock_mode: bool = True) -> None:
        self._connection_string = connection_string
        self._mock_mode = mock_mode
        self._connected = False
        # Mock data store
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._columns: dict[str, list[ColumnProfile]] = {}

    def connect(self) -> bool:
        """Test connection and verify read access."""
        if self._mock_mode:
            self._connected = True
            return True
        raise NotImplementedError("Production Postgres connection not yet implemented")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def register_mock_table(
        self,
        name: str,
        columns: list[ColumnProfile],
        rows: list[dict[str, Any]],
    ) -> None:
        """Register a table in mock mode for testing."""
        self._tables[name] = rows
        self._columns[name] = columns

    def list_tables(self) -> list[str]:
        if not self._connected:
            raise RuntimeError("Not connected")
        return list(self._tables.keys())

    def get_columns(self, table: str) -> list[ColumnProfile]:
        if not self._connected:
            raise RuntimeError("Not connected")
        return self._columns.get(table, [])

    def count_rows(self, table: str) -> int:
        if not self._connected:
            raise RuntimeError("Not connected")
        return len(self._tables.get(table, []))

    def sample_rows(self, table: str, limit: int = 5) -> list[dict[str, Any]]:
        if not self._connected:
            raise RuntimeError("Not connected")
        return self._tables.get(table, [])[:limit]

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a read-only SQL query. Mock mode returns from in-memory tables."""
        if not self._connected:
            raise RuntimeError("Not connected")

        if self._mock_mode:
            return self._mock_execute(sql)
        raise NotImplementedError("Production query execution not yet implemented")

    def _mock_execute(self, sql: str) -> list[dict[str, Any]]:
        """Simple mock SQL execution — just returns table data."""
        sql_lower = sql.lower().strip()

        # Safety check
        for forbidden in ("insert", "update", "delete", "drop", "create", "alter", "truncate"):
            if sql_lower.startswith(forbidden):
                raise PermissionError(f"Read-only connector: {forbidden.upper()} not allowed")

        # Find which table is referenced
        for table_name, rows in self._tables.items():
            if table_name.lower() in sql_lower:
                # Check for COUNT
                if "count(" in sql_lower or "count (*)" in sql_lower:
                    return [{"count": len(rows)}]
                # Check for LIMIT
                limit = self._extract_limit(sql_lower)
                return rows[:limit] if limit else rows

        return []

    @staticmethod
    def _extract_limit(sql: str) -> int | None:
        parts = sql.split()
        for i, part in enumerate(parts):
            if part == "limit" and i + 1 < len(parts):
                try:
                    return int(parts[i + 1].rstrip(";"))
                except ValueError:
                    pass
        return None

    def profile_schema(self) -> SchemaProfile:
        """Profile all tables in the database."""
        tables = []
        for name in self.list_tables():
            columns = self.get_columns(name)
            row_count = self.count_rows(name)
            sample = self.sample_rows(name, limit=5)

            tables.append(TableProfile(
                name=name,
                columns=columns,
                row_count=row_count,
                sample=sample,
                detected_keys=_detect_keys(columns),
                detected_time_columns=_detect_time_columns(columns),
            ))

        return SchemaProfile(tables=tables)


def _detect_keys(columns: list[ColumnProfile]) -> list[str]:
    """Detect likely primary/foreign keys from column names."""
    keys = []
    for col in columns:
        name = col.name.lower()
        if col.is_primary_key:
            keys.append(col.name)
        elif name == "id" or name.endswith("_id"):
            keys.append(col.name)
    return keys


def _detect_time_columns(columns: list[ColumnProfile]) -> list[str]:
    """Detect timestamp/date columns."""
    time_types = {"timestamp", "timestamptz", "date", "datetime"}
    time_suffixes = ("_at", "_date", "_time", "_ts")
    result = []
    for col in columns:
        if col.data_type.lower() in time_types:
            result.append(col.name)
        elif any(col.name.lower().endswith(s) for s in time_suffixes):
            result.append(col.name)
    return result
