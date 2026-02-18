from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class NeonProject:
    """Represents a Neon project (mock or real)."""

    def __init__(
        self,
        project_id: str,
        org_id: str | None = None,
        last_activity: datetime | None = None,
        tier: str = "free",
        suspended: bool = False,
        deleted: bool = False,
    ) -> None:
        self.project_id = project_id
        self.org_id = org_id
        self.last_activity = last_activity or datetime.now(timezone.utc)
        self.tier = tier
        self.suspended = suspended
        self.deleted = deleted


class NeonCleanup:
    """Auto-pause inactive projects, auto-delete abandoned ones."""

    def __init__(self) -> None:
        self._notifications: list[dict[str, Any]] = []

    def run_cleanup(
        self, projects: list[NeonProject], now: datetime | None = None
    ) -> dict[str, list[str]]:
        """Run cleanup pass. Returns dict of actions taken."""
        if now is None:
            now = datetime.now(timezone.utc)

        suspended: list[str] = []
        warned: list[str] = []
        deleted: list[str] = []

        for project in projects:
            if project.deleted:
                continue

            # Orphaned project (no org)
            if project.org_id is None:
                project.deleted = True
                deleted.append(project.project_id)
                logger.info("Deleted orphaned project %s", project.project_id)
                continue

            days_inactive = (now - project.last_activity).days

            if project.tier == "free" and days_inactive > 7:
                if not project.suspended:
                    project.suspended = True
                    suspended.append(project.project_id)
                    logger.info(
                        "Suspended free project %s (inactive %d days)",
                        project.project_id, days_inactive,
                    )
            elif project.tier == "pro" and days_inactive > 14:
                if not project.suspended:
                    project.suspended = True
                    suspended.append(project.project_id)
                    logger.info(
                        "Suspended pro project %s (inactive %d days)",
                        project.project_id, days_inactive,
                    )

            if days_inactive > 90:
                self._notifications.append({
                    "type": "deletion_warning",
                    "project_id": project.project_id,
                    "org_id": project.org_id,
                    "days_inactive": days_inactive,
                })
                warned.append(project.project_id)

                if days_inactive > 97:  # 7-day grace after warning
                    project.deleted = True
                    deleted.append(project.project_id)
                    logger.info(
                        "Deleted project %s after 97+ day inactivity",
                        project.project_id,
                    )

        return {"suspended": suspended, "warned": warned, "deleted": deleted}

    @property
    def notifications(self) -> list[dict[str, Any]]:
        return self._notifications
