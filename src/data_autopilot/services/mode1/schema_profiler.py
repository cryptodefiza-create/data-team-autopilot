from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ColumnProfile, SchemaProfile, TableProfile

logger = logging.getLogger(__name__)


class SchemaProfiler:
    """Profiles database schemas to discover tables, columns, relationships."""

    def profile(self, connector: Any) -> SchemaProfile:
        """Profile all tables using the given connector."""
        tables = connector.list_tables()
        all_columns: dict[str, list[ColumnProfile]] = {}
        profiles: list[TableProfile] = []

        for table in tables:
            columns = connector.get_columns(table)
            all_columns[table] = columns

        for table in tables:
            columns = all_columns[table]
            row_count = connector.count_rows(table)
            sample = connector.sample_rows(table, limit=5)

            relationships = self.detect_relationships(columns, tables, all_columns)

            profiles.append(TableProfile(
                name=table,
                columns=columns,
                row_count=row_count,
                sample=sample,
                detected_keys=self._detect_keys(columns),
                detected_time_columns=self._detect_time_columns(columns),
                detected_relationships=relationships,
            ))

        schema = SchemaProfile(tables=profiles)
        logger.info(
            "Profiled %d tables, %d total columns",
            len(profiles),
            sum(len(t.columns) for t in profiles),
        )
        return schema

    def detect_relationships(
        self,
        columns: list[ColumnProfile],
        all_tables: list[str],
        all_columns: dict[str, list[ColumnProfile]],
    ) -> list[dict[str, str]]:
        """Detect foreign key relationships by matching column names."""
        relationships = []

        for col in columns:
            name = col.name.lower()
            if not name.endswith("_id"):
                continue

            # e.g. "user_id" → look for table "users" or "user"
            prefix = name[:-3]  # strip "_id"
            for table in all_tables:
                table_lower = table.lower()
                if table_lower == prefix or table_lower == prefix + "s":
                    # Check the target table has an "id" column
                    target_cols = all_columns.get(table, [])
                    if any(c.name.lower() == "id" for c in target_cols):
                        relationships.append({
                            "column": col.name,
                            "references_table": table,
                            "references_column": "id",
                        })
                        break

        return relationships

    @staticmethod
    def _detect_keys(columns: list[ColumnProfile]) -> list[str]:
        keys = []
        for col in columns:
            if col.is_primary_key:
                keys.append(col.name)
            elif col.name.lower() == "id" or col.name.lower().endswith("_id"):
                keys.append(col.name)
        return keys

    @staticmethod
    def _detect_time_columns(columns: list[ColumnProfile]) -> list[str]:
        time_types = {"timestamp", "timestamptz", "date", "datetime"}
        time_suffixes = ("_at", "_date", "_time", "_ts")
        result = []
        for col in columns:
            if col.data_type.lower() in time_types:
                result.append(col.name)
            elif any(col.name.lower().endswith(s) for s in time_suffixes):
                result.append(col.name)
        return result
