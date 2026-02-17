from data_autopilot.services.memo_service import MemoService


def test_memo_validation_rejects_unknown_metric() -> None:
    svc = MemoService()
    packet = {
        "kpis": [
            {
                "metric_name": "DAU",
                "current_value": 10,
                "previous_value": 8,
                "delta_percent": 25,
                "significance": "notable",
                "query_hash": "q1",
            }
        ]
    }
    memo = {
        "key_changes": [{"metric_name": "Churn", "current": 10, "previous": 8, "delta_pct": 25}],
        "likely_causes": [],
    }
    result = svc.validate(packet, memo)
    assert not result.passed
    assert any("Unknown metric" in e for e in result.errors)


def test_memo_validation_downgrades_unsupported_data_supported_cause() -> None:
    svc = MemoService()
    packet = {
        "kpis": [
            {
                "metric_name": "DAU",
                "current_value": 10,
                "previous_value": 8,
                "delta_percent": 25,
                "significance": "notable",
                "query_hash": "q1",
            }
        ],
        "top_segments": [{"segment": "organic_search"}],
    }
    memo = {
        "key_changes": [{"metric_name": "DAU", "current": 10, "previous": 8, "delta_pct": 25}],
        "likely_causes": [
            {
                "hypothesis": "Something changed",
                "supporting_evidence": "backend outage",
                "evidence_type": "data_supported",
            }
        ],
    }
    result = svc.validate(packet, memo)
    assert result.passed
    assert any("Downgraded unsupported" in w for w in result.warnings)
    assert memo["likely_causes"][0]["evidence_type"] == "speculative"
