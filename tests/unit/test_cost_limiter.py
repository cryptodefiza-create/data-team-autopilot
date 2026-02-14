from data_autopilot.services.cost_limiter import SlidingWindowCostLimiter
from data_autopilot.services.redis_store import RedisStore


def test_cost_limiter_blocks_when_budget_exceeded() -> None:
    store = RedisStore("redis://localhost:6379/0")
    limiter = SlidingWindowCostLimiter(store=store)

    org_id = "org_test"
    near_budget = limiter.settings.org_hourly_budget_bytes - 1024
    limiter.record(org_id, near_budget)

    status = limiter.check(org_id, 2048)
    assert not status.allowed
    assert status.bytes_remaining <= 1024
