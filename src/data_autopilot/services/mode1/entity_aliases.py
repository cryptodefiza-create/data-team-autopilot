from __future__ import annotations

import logging

from data_autopilot.services.mode1.models import EntityAlias

logger = logging.getLogger(__name__)


class EntityAliasManager:
    """Maps cryptic table names to human-readable aliases.

    Data engineers set aliases at connection time (e.g. 'fct_orders_v2_final' → 'Orders').
    When a user asks about 'orders', the agent resolves it to the actual table name.
    """

    def __init__(self) -> None:
        self._aliases: dict[str, list[EntityAlias]] = {}  # org_id -> aliases

    def set_alias(self, org_id: str, table_name: str, alias: str) -> None:
        if org_id not in self._aliases:
            self._aliases[org_id] = []

        # Update existing or add new
        for a in self._aliases[org_id]:
            if a.table_name == table_name:
                a.alias = alias
                logger.info("Updated alias: %s → %s (org %s)", table_name, alias, org_id)
                return

        self._aliases[org_id].append(
            EntityAlias(table_name=table_name, alias=alias, org_id=org_id)
        )
        logger.info("Set alias: %s → %s (org %s)", table_name, alias, org_id)

    def resolve(self, org_id: str, user_term: str) -> str | None:
        """Resolve a user term to a table name. Case-insensitive."""
        term_lower = user_term.lower().strip()
        for alias in self._aliases.get(org_id, []):
            if alias.alias.lower() == term_lower:
                return alias.table_name
            if alias.table_name.lower() == term_lower:
                return alias.table_name
        return None

    def get_aliases(self, org_id: str) -> list[EntityAlias]:
        return self._aliases.get(org_id, [])

    def get_table_for_query(self, org_id: str, query_text: str) -> str | None:
        """Find a table name mentioned in a user query (by alias or table name)."""
        text_lower = query_text.lower()
        for alias in self._aliases.get(org_id, []):
            if alias.alias.lower() in text_lower:
                return alias.table_name
            if alias.table_name.lower() in text_lower:
                return alias.table_name
        return None

    def set_aliases_bulk(self, org_id: str, mapping: dict[str, str]) -> int:
        """Set multiple aliases at once. Returns count set."""
        for table_name, alias in mapping.items():
            self.set_alias(org_id, table_name, alias)
        return len(mapping)
