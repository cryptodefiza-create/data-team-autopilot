"""Comprehensive tests for memo validation hardening.

Tests all 4 validation checks, regeneration logic, adversarial cases,
and the demo packet for provider evaluation.
"""
from __future__ import annotations

from data_autopilot.services.memo_service import (
    MemoService,
    _collect_packet_values,
    validate_causes,
    validate_coverage,
    validate_metric_names,
    validate_numbers,
)


def _make_packet(**overrides) -> dict:
    """Standard test packet with 3 KPIs."""
    packet = {
        "kpis": [
            {
                "metric_name": "DAU",
                "current_value": 12450,
                "previous_value": 11200,
                "delta_absolute": 1250,
                "delta_percent": 11.16,
                "significance": "notable",
                "query_hash": "q_dau",
            },
            {
                "metric_name": "Revenue",
                "current_value": 84320.50,
                "previous_value": 78900.00,
                "delta_absolute": 5420.50,
                "delta_percent": 6.87,
                "significance": "normal",
                "query_hash": "q_revenue",
            },
            {
                "metric_name": "Conversion Rate",
                "current_value": 3.42,
                "previous_value": 2.91,
                "delta_absolute": 0.51,
                "delta_percent": 17.53,
                "significance": "notable",
                "query_hash": "q_conversion",
            },
        ],
        "top_segments": [
            {"segment": "organic_search", "delta_contribution_pct": 42.3},
            {"segment": "paid_social", "delta_contribution_pct": 28.1},
        ],
        "anomaly_notes": ["analytics.events table had 8-hour delay"],
    }
    packet.update(overrides)
    return packet


def _make_valid_memo(packet: dict) -> dict:
    """Build a memo that passes all validation checks."""
    return {
        "headline_summary": ["DAU up 11.16% week over week."],
        "key_changes": [
            {
                "metric_name": k["metric_name"],
                "current": k["current_value"],
                "previous": k["previous_value"],
                "delta_pct": k["delta_percent"],
                "delta_absolute": k["delta_absolute"],
                "interpretation": f"{k['metric_name']} changed.",
                "confidence": "high",
            }
            for k in packet["kpis"]
        ],
        "likely_causes": [
            {
                "hypothesis": "Growth in organic search traffic.",
                "supporting_evidence": "DAU increase correlated with organic_search segment growth.",
                "evidence_type": "data_supported",
            }
        ],
        "recommended_actions": ["Monitor conversion rate trends."],
        "data_quality_notes": packet["anomaly_notes"],
    }


# ── Check 1: Number reconciliation ──────────────────────────────────


def test_number_reconciliation_passes_with_exact_values() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    errors = validate_numbers(memo, packet)
    assert errors == []


def test_number_reconciliation_catches_wrong_current() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    memo["key_changes"][0]["current"] = 12500  # wrong: should be 12450
    errors = validate_numbers(memo, packet)
    assert len(errors) == 1
    assert "Current value mismatch for DAU" in errors[0]
    assert "12500" in errors[0]
    assert "12450" in errors[0]


def test_number_reconciliation_catches_wrong_previous() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    memo["key_changes"][1]["previous"] = 79000.00  # wrong: should be 78900.00
    errors = validate_numbers(memo, packet)
    assert len(errors) == 1
    assert "Previous value mismatch for Revenue" in errors[0]


def test_number_reconciliation_catches_wrong_delta_pct() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    memo["key_changes"][0]["delta_pct"] = 11.2  # wrong: should be 11.16
    errors = validate_numbers(memo, packet)
    assert len(errors) == 1
    assert "Delta percent mismatch for DAU" in errors[0]


def test_number_reconciliation_catches_wrong_delta_absolute() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    memo["key_changes"][0]["delta_absolute"] = 1300  # wrong: should be 1250
    errors = validate_numbers(memo, packet)
    assert len(errors) == 1
    assert "Delta absolute mismatch for DAU" in errors[0]


def test_number_reconciliation_skips_missing_fields() -> None:
    """If a field isn't present in memo, don't error — only check what's there."""
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    del memo["key_changes"][0]["delta_absolute"]
    errors = validate_numbers(memo, packet)
    assert errors == []


# ── Check 2: Metric coverage ────────────────────────────────────────


def test_coverage_passes_when_all_notable_present() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    warnings = validate_coverage(memo, packet)
    assert warnings == []


def test_coverage_warns_on_missing_notable_metric() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    memo["key_changes"] = [memo["key_changes"][0]]  # only DAU, missing Conversion Rate
    warnings = validate_coverage(memo, packet)
    assert len(warnings) == 1
    assert "Conversion Rate" in warnings[0]


def test_coverage_ignores_normal_significance() -> None:
    """Revenue has 'normal' significance — not required in coverage."""
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    # Remove Revenue from key_changes
    memo["key_changes"] = [c for c in memo["key_changes"] if c["metric_name"] != "Revenue"]
    warnings = validate_coverage(memo, packet)
    assert warnings == []  # Revenue is normal, not required


# ── Check 3: Hallucination detection ────────────────────────────────


def test_hallucination_rejects_unknown_metric() -> None:
    packet = _make_packet()
    memo = {"key_changes": [{"metric_name": "CHURN_RATE", "current": 5.2, "previous": 4.8}]}
    errors = validate_metric_names(memo, packet)
    assert len(errors) == 1
    assert "CHURN_RATE" in errors[0]


def test_hallucination_accepts_known_metrics() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    errors = validate_metric_names(memo, packet)
    assert errors == []


def test_hallucination_catches_multiple_fake_metrics() -> None:
    packet = _make_packet()
    memo = {
        "key_changes": [
            {"metric_name": "FAKE_METRIC_1"},
            {"metric_name": "FAKE_METRIC_2"},
            {"metric_name": "DAU", "current": 12450, "previous": 11200},
        ]
    }
    errors = validate_metric_names(memo, packet)
    assert len(errors) == 2


# ── Check 4: Cause evidence validation ──────────────────────────────


def test_cause_valid_data_supported() -> None:
    packet = _make_packet()
    memo = {
        "key_changes": [],
        "likely_causes": [
            {
                "hypothesis": "Organic growth up.",
                "supporting_evidence": "DAU increased significantly",
                "evidence_type": "data_supported",
            }
        ],
    }
    errors = validate_causes(memo, packet)
    assert errors == []


def test_cause_valid_speculative() -> None:
    packet = _make_packet()
    memo = {
        "key_changes": [],
        "likely_causes": [
            {
                "hypothesis": "Market trends favorable.",
                "supporting_evidence": "General market conditions",
                "evidence_type": "speculative",
            }
        ],
    }
    errors = validate_causes(memo, packet)
    assert errors == []


def test_cause_invalid_evidence_type() -> None:
    packet = _make_packet()
    memo = {
        "key_changes": [],
        "likely_causes": [
            {
                "hypothesis": "Something happened.",
                "supporting_evidence": "unknown",
                "evidence_type": "confirmed",
            }
        ],
    }
    errors = validate_causes(memo, packet)
    assert any("Invalid evidence_type" in e for e in errors)


def test_cause_data_supported_without_reference_gets_downgraded() -> None:
    packet = _make_packet()
    cause = {
        "hypothesis": "Server issues.",
        "supporting_evidence": "backend outage on Tuesday",
        "evidence_type": "data_supported",
    }
    memo = {"key_changes": [], "likely_causes": [cause]}
    errors = validate_causes(memo, packet)
    assert cause["evidence_type"] == "speculative"
    assert cause["supporting_evidence"] == "no supporting data"
    assert any("Downgraded" in e for e in errors)


def test_cause_data_supported_with_segment_reference_passes() -> None:
    packet = _make_packet()
    memo = {
        "key_changes": [],
        "likely_causes": [
            {
                "hypothesis": "Organic search growth.",
                "supporting_evidence": "organic_search segment contributed 42.3% of delta",
                "evidence_type": "data_supported",
            }
        ],
    }
    errors = validate_causes(memo, packet)
    assert errors == []


# ── Full validate() integration ─────────────────────────────────────


def test_full_validate_passes_valid_memo() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    svc = MemoService()
    result = svc.validate(packet, memo)
    assert result.passed is True
    assert result.errors == []


def test_full_validate_catches_combined_errors() -> None:
    packet = _make_packet()
    memo = _make_valid_memo(packet)
    # Inject hallucinated metric
    memo["key_changes"].append({"metric_name": "FAKE", "current": 999, "previous": 888})
    # Wrong number on DAU
    memo["key_changes"][0]["current"] = 99999
    svc = MemoService()
    result = svc.validate(packet, memo)
    assert result.passed is False
    assert any("FAKE" in e for e in result.errors)
    assert any("Current value mismatch" in e for e in result.errors)


# ── _collect_packet_values ──────────────────────────────────────────


def test_collect_packet_values_includes_all_types() -> None:
    packet = _make_packet()
    values = _collect_packet_values(packet)
    assert 12450 in values
    assert 11200 in values
    assert 1250 in values
    assert 11.16 in values
    assert 84320.50 in values
    # Segment values
    assert 42.3 in values
    assert 28.1 in values


# ── Fallback memo passes validation ─────────────────────────────────


def test_fallback_memo_passes_validation() -> None:
    """The deterministic fallback memo should always pass validation."""
    packet = _make_packet()
    svc = MemoService()
    fallback = svc._generate_memo_fallback(packet)
    result = svc.validate(packet, fallback)
    assert result.passed is True, f"Fallback memo failed validation: {result.errors}"


# ── System prompt with corrections ──────────────────────────────────


def test_build_system_prompt_includes_corrections() -> None:
    svc = MemoService()
    prompt = svc._build_system_prompt(correction_errors=["Wrong DAU value", "Unknown metric X"])
    assert "PREVIOUS ATTEMPT FAILED VALIDATION" in prompt
    assert "Wrong DAU value" in prompt
    assert "Unknown metric X" in prompt


def test_build_system_prompt_without_corrections() -> None:
    svc = MemoService()
    prompt = svc._build_system_prompt()
    assert "PREVIOUS ATTEMPT FAILED VALIDATION" not in prompt
    assert "CRITICAL RULES" in prompt


# ── Demo packet ─────────────────────────────────────────────────────


def test_demo_packet_is_valid() -> None:
    from data_autopilot.api.core_routes import _demo_memo_packet

    packet = _demo_memo_packet()
    assert len(packet["kpis"]) == 3
    assert len(packet["top_segments"]) == 2
    for kpi in packet["kpis"]:
        for key in ("metric_name", "current_value", "previous_value", "delta_absolute", "delta_percent"):
            assert key in kpi, f"KPI missing key: {key}"

    # Fallback memo from demo packet should pass validation
    svc = MemoService()
    fallback = svc._generate_memo_fallback(packet)
    result = svc.validate(packet, fallback)
    assert result.passed is True
