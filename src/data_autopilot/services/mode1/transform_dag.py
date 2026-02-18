from __future__ import annotations

import logging
from typing import Any, Callable

from data_autopilot.services.mode1.models import DAGNode

logger = logging.getLogger(__name__)


class TransformDAG:
    """Manages transformation dependency ordering: raw → staging → marts.

    Ensures staging runs before marts. If staging fails, dependent marts are blocked.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, DAGNode] = {}

    def add_node(
        self, name: str, layer: str, depends_on: list[str] | None = None
    ) -> DAGNode:
        node = DAGNode(
            name=name,
            layer=layer,
            depends_on=depends_on or [],
        )
        self._nodes[name] = node
        return node

    def get_node(self, name: str) -> DAGNode | None:
        return self._nodes.get(name)

    def get_execution_order(self) -> list[str]:
        """Return nodes in topological order (dependencies first)."""
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            node = self._nodes.get(name)
            if node:
                for dep in node.depends_on:
                    visit(dep)
                order.append(name)

        for name in self._nodes:
            visit(name)

        return order

    def execute(
        self, runners: dict[str, Callable[[], Any]]
    ) -> dict[str, Any]:
        """Execute all nodes in dependency order.

        Args:
            runners: dict mapping node name → callable that runs the transformation.

        Returns:
            dict with execution results per node.
        """
        order = self.get_execution_order()
        results: dict[str, Any] = {}

        for name in order:
            node = self._nodes[name]

            # Check if any dependency failed
            blocked_by = self._check_blocked(node)
            if blocked_by:
                node.status = "failed"
                node.error = f"Blocked by failed dependency: {blocked_by}"
                results[name] = {"status": "blocked", "blocked_by": blocked_by}
                logger.warning("Node %s blocked by %s", name, blocked_by)
                continue

            runner = runners.get(name)
            if runner is None:
                node.status = "completed"
                results[name] = {"status": "skipped", "reason": "no runner"}
                continue

            node.status = "running"
            try:
                result = runner()
                node.status = "completed"
                results[name] = {"status": "completed", "result": result}
                logger.info("Node %s completed", name)
            except Exception as exc:
                node.status = "failed"
                node.error = str(exc)
                results[name] = {"status": "failed", "error": str(exc)}
                logger.error("Node %s failed: %s", name, exc)

        return results

    def _check_blocked(self, node: DAGNode) -> str | None:
        """Check if any dependency has failed."""
        for dep_name in node.depends_on:
            dep = self._nodes.get(dep_name)
            if dep and dep.status == "failed":
                return dep_name
        return None

    def reset(self) -> None:
        """Reset all nodes to pending."""
        for node in self._nodes.values():
            node.status = "pending"
            node.error = None

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    def get_status_summary(self) -> dict[str, int]:
        summary: dict[str, int] = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
        for node in self._nodes.values():
            summary[node.status] = summary.get(node.status, 0) + 1
        return summary
