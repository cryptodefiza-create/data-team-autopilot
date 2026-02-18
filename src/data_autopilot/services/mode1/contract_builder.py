from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    ContractDefaults,
    EntityConfig,
    MetricDefinition,
    SchemaProfile,
    SemanticContract,
)

logger = logging.getLogger(__name__)

# Questions the builder asks to construct a contract
CONTRACT_QUESTIONS = [
    {
        "id": "grain",
        "question": "What does one row in your orders table represent?",
        "options": [
            {"label": "One order (with multiple line items elsewhere)", "value": "one_order"},
            {"label": "One line item per row", "value": "one_line_item"},
        ],
    },
    {
        "id": "revenue",
        "question": "How do you calculate revenue?",
        "options": [
            {"label": "Sum of order amounts (gross)", "value": "gross"},
            {"label": "Sum of order amounts minus refunds (net)", "value": "net_after_refunds"},
            {"label": "Net after refunds and tax", "value": "net_after_refunds_and_tax"},
        ],
    },
    {
        "id": "active_customer",
        "question": "What makes a customer 'active'?",
        "options": [
            {"label": "Placed an order in last 90 days", "value": "order_90d"},
            {"label": "Logged in in last 30 days", "value": "login_30d"},
            {"label": "Has an active subscription", "value": "active_sub"},
        ],
    },
]


class ConversationalContractBuilder:
    """Builds a semantic contract through a question-and-answer flow."""

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}  # org_id -> session state

    def start_session(self, org_id: str, schema: SchemaProfile | None = None) -> dict[str, Any]:
        """Start a contract-building session. Returns first question."""
        self._sessions[org_id] = {
            "answers": {},
            "current_question": 0,
            "schema": schema,
            "completed": False,
        }
        return {
            "status": "in_progress",
            "question": CONTRACT_QUESTIONS[0],
            "progress": f"1/{len(CONTRACT_QUESTIONS)}",
        }

    def answer(self, org_id: str, question_id: str, answer: str) -> dict[str, Any]:
        """Submit an answer to the current question. Returns next question or contract."""
        session = self._sessions.get(org_id)
        if session is None:
            return {"status": "error", "message": "No active session. Call start_session first."}

        session["answers"][question_id] = answer
        session["current_question"] += 1

        if session["current_question"] >= len(CONTRACT_QUESTIONS):
            # All questions answered — build contract
            contract = self._build_contract(org_id, session["answers"], session.get("schema"))
            session["completed"] = True
            return {
                "status": "completed",
                "contract": contract,
            }

        next_q = CONTRACT_QUESTIONS[session["current_question"]]
        return {
            "status": "in_progress",
            "question": next_q,
            "progress": f"{session['current_question'] + 1}/{len(CONTRACT_QUESTIONS)}",
        }

    def is_complete(self, org_id: str) -> bool:
        session = self._sessions.get(org_id)
        return session is not None and session.get("completed", False)

    def _build_contract(
        self, org_id: str, answers: dict[str, str], schema: SchemaProfile | None
    ) -> SemanticContract:
        """Build a semantic contract from collected answers."""
        grain = answers.get("grain", "one_order")
        revenue_def = answers.get("revenue", "gross")

        # Determine revenue SQL expression
        if revenue_def == "gross":
            rev_sql = "SUM(order_amount)"
        elif revenue_def == "net_after_refunds":
            rev_sql = "SUM(order_amount) - SUM(refund_amount)"
        else:
            rev_sql = "SUM(order_amount) - SUM(refund_amount) - SUM(tax_amount)"

        entities = []
        if schema:
            for table in schema.tables:
                entities.append(EntityConfig(
                    name=table.name,
                    grain=f"one row per {grain}" if grain == "one_order" else "one line item per row",
                    source_table=f"staging.stg_{table.name}",
                    primary_key=table.detected_keys[0] if table.detected_keys else "id",
                ))
        else:
            entities.append(EntityConfig(
                name="order",
                grain=f"one row per {grain}",
                source_table="staging.stg_orders",
                primary_key="order_id",
            ))

        metrics = [
            MetricDefinition(
                name="revenue",
                definition=rev_sql,
                includes_tax=(revenue_def == "net_after_refunds_and_tax"),
                includes_refunds=(revenue_def != "gross"),
            ),
        ]

        return SemanticContract(
            org_id=org_id,
            version=1,
            entities=entities,
            metrics=metrics,
            defaults=ContractDefaults(),
        )
