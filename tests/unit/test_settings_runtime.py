import pytest

from data_autopilot.config.settings import Settings


def test_reject_real_execution_with_mock_bigquery() -> None:
    with pytest.raises(ValueError):
        Settings(allow_real_query_execution=True, bigquery_mock_mode=True)


def test_require_metabase_key_when_live() -> None:
    with pytest.raises(ValueError):
        Settings(metabase_mock_mode=False, metabase_url="http://localhost:3000", metabase_api_key="")


def test_require_bigquery_project_when_live() -> None:
    with pytest.raises(ValueError):
        Settings(bigquery_mock_mode=False, bigquery_project_id="")
