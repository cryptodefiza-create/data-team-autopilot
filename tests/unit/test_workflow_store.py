from data_autopilot.tools.workflows.engine import InMemoryWorkflowStore, WorkflowStepState


def test_store_idempotency_key_roundtrip() -> None:
    store = InMemoryWorkflowStore()
    key = store.key("org_1", "wf_1", "step_1", {"a": 1})
    state = WorkflowStepState(step_name="step_1", status="success", output={"ok": True})
    store.put(key, state)
    loaded = store.get(key)
    assert loaded is not None
    assert loaded.status == "success"
