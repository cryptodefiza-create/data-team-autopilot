from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_ready_mock_mode() -> None:
    r = client.get('/ready')
    assert r.status_code == 200
    body = r.json()
    assert body['ok'] is True
    assert body['checks']['bigquery']['mode'] == 'mock'
    assert body['checks']['metabase']['mode'] == 'mock'
