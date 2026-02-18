from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.models import (
    AgentManagerConfig,
    TeamMember,
    TeamRole,
)

logger = logging.getLogger(__name__)


class AgentManager:
    """Data engineer configures the agent for their team.

    Controls:
    - Which tables/schemas the agent can access
    - Entity aliases
    - Who can ask questions (viewer) vs modify definitions (admin/engineer)
    - Delivery channel configuration
    """

    def __init__(self, alias_manager: EntityAliasManager | None = None) -> None:
        self._aliases = alias_manager or EntityAliasManager()
        self._configs: dict[str, AgentManagerConfig] = {}  # org_id -> config
        self._members: dict[str, list[TeamMember]] = {}  # org_id -> members

    def configure(self, org_id: str, config: AgentManagerConfig) -> None:
        """Set or update agent configuration for an org."""
        config.org_id = org_id
        self._configs[org_id] = config
        logger.info("Configured agent manager for org %s", org_id)

    def get_config(self, org_id: str) -> AgentManagerConfig | None:
        return self._configs.get(org_id)

    def add_member(self, org_id: str, member: TeamMember) -> None:
        """Add a team member with a role."""
        member.org_id = org_id
        if org_id not in self._members:
            self._members[org_id] = []

        # Update if exists
        for i, m in enumerate(self._members[org_id]):
            if m.user_id == member.user_id:
                self._members[org_id][i] = member
                return

        self._members[org_id].append(member)

    def get_member(self, org_id: str, user_id: str) -> TeamMember | None:
        for m in self._members.get(org_id, []):
            if m.user_id == user_id:
                return m
        return None

    def get_members(self, org_id: str) -> list[TeamMember]:
        return self._members.get(org_id, [])

    def check_permission(
        self, org_id: str, user_id: str, action: str
    ) -> bool:
        """Check if a user has permission for an action.

        Permissions:
        - viewer: can query, view dashboards
        - engineer: can set aliases, configure schemas
        - admin: can modify contracts, manage team, configure delivery
        """
        member = self.get_member(org_id, user_id)
        if member is None:
            return False

        if member.role == TeamRole.ADMIN:
            return True

        if member.role == TeamRole.ENGINEER:
            return action in {
                "query", "view_dashboard", "set_alias",
                "configure_schema", "view_contract",
            }

        if member.role == TeamRole.VIEWER:
            return action in {"query", "view_dashboard"}

        return False

    def set_aliases(
        self, org_id: str, user_id: str, aliases: dict[str, str]
    ) -> dict[str, Any]:
        """Set entity aliases (requires engineer or admin role)."""
        if not self.check_permission(org_id, user_id, "set_alias"):
            return {
                "status": "blocked",
                "message": "Only engineers and admins can modify aliases.",
            }

        count = self._aliases.set_aliases_bulk(org_id, aliases)
        return {
            "status": "success",
            "aliases_set": count,
            "message": f"Set {count} alias(es).",
        }

    def modify_contract(
        self, org_id: str, user_id: str
    ) -> dict[str, Any]:
        """Check if user can modify contract (requires admin role)."""
        if not self.check_permission(org_id, user_id, "modify_contract"):
            return {
                "status": "blocked",
                "message": "Only admins can modify contract definitions.",
            }
        return {"status": "allowed"}
