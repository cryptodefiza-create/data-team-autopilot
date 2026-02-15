from __future__ import annotations

from data_autopilot.config.settings import Settings
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.metabase_client import MetabaseClient


def run_startup_checks(settings: Settings) -> None:
    if not settings.run_startup_connection_tests:
        return

    if not settings.bigquery_mock_mode:
        bq = BigQueryConnector()
        status = bq.test_connection()
        if not status.get("ok"):
            raise RuntimeError("BigQuery connection test failed")

    if not settings.metabase_mock_mode:
        mb = MetabaseClient()
        status = mb.test_connection()
        if not status.get("ok"):
            raise RuntimeError("Metabase connection test failed")
