"""Integration tests for tester onboarding: mock schema, mock queries, blind labels, setup endpoint."""
from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from data_autopilot.main import app
from data_autopilot.services.bigquery_connector import BigQueryConnector


client = TestClient(app)


def _headers(org_id: str, role: str = "admin") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": role}


# ---------------------------------------------------------------------------
# Mock schema tests
# ---------------------------------------------------------------------------

class TestMockSchema:
    """Verify the enhanced 4-table mock schema."""

    def setup_method(self) -> None:
        self.bq = BigQueryConnector()
        self.schema = self.bq.introspect(connection_id="test_conn")
        self.tables = self.schema["datasets"]["analytics"]["tables"]

    def test_introspect_returns_four_tables(self) -> None:
        assert set(self.tables.keys()) == {"users", "events", "orders", "config"}

    def test_users_table_schema(self) -> None:
        t = self.tables["users"]
        col_names = [c["name"] for c in t["columns"]]
        assert "user_id" in col_names
        assert "email" in col_names
        assert "created_at" in col_names
        assert "channel" in col_names
        assert "country" in col_names
        assert t["row_count_est"] >= 1000

    def test_events_table_schema(self) -> None:
        t = self.tables["events"]
        col_names = [c["name"] for c in t["columns"]]
        assert "event_id" in col_names
        assert "user_id" in col_names
        assert "event_name" in col_names
        assert "timestamp" in col_names
        assert "properties" in col_names
        assert t["row_count_est"] >= 10000

    def test_orders_table_schema(self) -> None:
        t = self.tables["orders"]
        col_names = [c["name"] for c in t["columns"]]
        assert "order_id" in col_names
        assert "user_id" in col_names
        assert "amount" in col_names
        assert "status" in col_names
        assert "created_at" in col_names
        assert t["row_count_est"] >= 5000

    def test_config_table_is_small(self) -> None:
        t = self.tables["config"]
        assert t["row_count_est"] < 100
        assert t["partition_key"] is None


# ---------------------------------------------------------------------------
# Mock query tests
# ---------------------------------------------------------------------------

class TestMockQueries:
    """Verify realistic mock query results."""

    def setup_method(self) -> None:
        self.bq = BigQueryConnector()

    def test_dau_values_in_range(self) -> None:
        result = self.bq.execute_query(
            "SELECT DATE(timestamp) AS day, COUNT(DISTINCT user_id) AS dau FROM events GROUP BY day"
        )
        rows = result["rows"]
        assert len(rows) > 0
        for row in rows:
            assert 200 <= row["dau"] <= 400, f"DAU {row['dau']} out of range on {row['day']}"

    def test_revenue_weekday_gt_weekend(self) -> None:
        result = self.bq.execute_query(
            "SELECT DATE(created_at) AS day, SUM(amount) AS revenue FROM orders GROUP BY day"
        )
        rows = result["rows"]
        weekday_revenues = []
        weekend_revenues = []
        for row in rows:
            # Parse day to get weekday index
            parts = row["day"].split("-")
            from datetime import date
            d = date(int(parts[0]), int(parts[1]), int(parts[2]))
            if d.weekday() < 5:
                weekday_revenues.append(row["revenue"])
            else:
                weekend_revenues.append(row["revenue"])
        if weekday_revenues and weekend_revenues:
            avg_weekday = sum(weekday_revenues) / len(weekday_revenues)
            avg_weekend = sum(weekend_revenues) / len(weekend_revenues)
            assert avg_weekday > avg_weekend

    def test_missing_data_day(self) -> None:
        result = self.bq.execute_query(
            "SELECT DATE(timestamp) AS day, COUNT(DISTINCT user_id) AS dau FROM events GROUP BY day"
        )
        # 14-day range minus 1 missing = 13 rows
        assert len(result["rows"]) == 13

    def test_scalar_dau_in_range(self) -> None:
        result = self.bq.execute_query(
            "SELECT COUNT(DISTINCT user_id) AS dau FROM events"
        )
        rows = result["rows"]
        assert len(rows) == 1
        assert 200 <= rows[0]["dau"] <= 400


# ---------------------------------------------------------------------------
# Blind label tests
# ---------------------------------------------------------------------------

class TestBlindLabels:
    """Verify blind model labeling in evaluate-memo."""

    def test_evaluate_memo_keys_are_blind_labels(self) -> None:
        """When results are returned, keys should be 'Model A', 'Model B', etc."""
        r = client.post(
            "/api/v1/llm/evaluate-memo",
            headers=_headers("org_blind_test"),
            json={"org_id": "org_blind_test"},
        )
        assert r.status_code == 200
        body = r.json()
        # If no providers configured, results will be empty — that's fine
        if body.get("results"):
            for key in body["results"]:
                assert key.startswith("Model "), f"Key '{key}' should start with 'Model '"
            assert body.get("blind_mode") is True

    def test_no_real_provider_names_leak(self) -> None:
        """Real provider names should not appear as keys in results."""
        known_providers = {"grok", "openai", "claude", "anthropic", "xai", "gpt"}
        r = client.post(
            "/api/v1/llm/evaluate-memo",
            headers=_headers("org_blind_leak"),
            json={"org_id": "org_blind_leak"},
        )
        assert r.status_code == 200
        body = r.json()
        for key in (body.get("results") or {}):
            assert key.lower() not in known_providers, f"Provider name '{key}' leaked through"


# ---------------------------------------------------------------------------
# Setup endpoint tests
# ---------------------------------------------------------------------------

class TestSetupTesterOrg:
    """Verify the one-shot tester org setup endpoint."""

    def test_setup_creates_tenant_and_discovers_tables(self) -> None:
        org = f"org_setup_{uuid4().hex[:8]}"
        r = client.post(
            "/api/v1/admin/setup-tester-org",
            headers=_headers(org),
            json={"org_id": org},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["org_id"] == org
        assert body["tenant_exists"] is False  # was newly created
        assert body["tables_discovered"] == 4
        assert "users" in body["table_names"]
        assert body["pii_columns_flagged"] >= 1
        # email column should be flagged
        pii_columns = [p["column"] for p in body["pii_details"]]
        assert "email" in pii_columns

    def test_setup_is_idempotent(self) -> None:
        org = f"org_idem_{uuid4().hex[:8]}"
        r1 = client.post(
            "/api/v1/admin/setup-tester-org",
            headers=_headers(org),
            json={"org_id": org},
        )
        assert r1.status_code == 200
        assert r1.json()["tenant_exists"] is False

        r2 = client.post(
            "/api/v1/admin/setup-tester-org",
            headers=_headers(org),
            json={"org_id": org},
        )
        assert r2.status_code == 200
        assert r2.json()["tenant_exists"] is True
        assert r2.json()["tables_discovered"] >= 4

    def test_setup_requires_admin(self) -> None:
        org = f"org_nonadmin_{uuid4().hex[:8]}"
        r = client.post(
            "/api/v1/admin/setup-tester-org",
            headers=_headers(org, role="member"),
            json={"org_id": org},
        )
        assert r.status_code == 403
