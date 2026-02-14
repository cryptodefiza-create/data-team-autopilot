from data_autopilot.services.sql_safety import SqlSafetyEngine


def test_blocks_multi_statement() -> None:
    engine = SqlSafetyEngine()
    decision = engine.evaluate("SELECT 1; DROP TABLE users")
    assert not decision.allowed


def test_blocks_ddl() -> None:
    engine = SqlSafetyEngine()
    decision = engine.evaluate("CREATE TABLE t(id INT)")
    assert not decision.allowed


def test_adds_limit_on_non_aggregate_select() -> None:
    engine = SqlSafetyEngine(default_limit=10000)
    decision = engine.evaluate("SELECT id FROM users")
    assert decision.allowed
    assert "LIMIT 10000" in decision.rewritten_sql


def test_blocks_excessive_join_depth() -> None:
    engine = SqlSafetyEngine(max_join_depth=2)
    decision = engine.evaluate(
        "SELECT * FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id JOIN d ON c.id=d.id"
    )
    assert not decision.allowed
    assert "Join depth exceeds max" in decision.reasons[0]


def test_blocks_excessive_subquery_nesting() -> None:
    engine = SqlSafetyEngine(max_subquery_depth=1)
    decision = engine.evaluate(
        "SELECT * FROM (SELECT * FROM (SELECT id FROM users) u1) u2"
    )
    assert not decision.allowed
    assert "Subquery nesting exceeds max" in decision.reasons[0]


def test_blocks_dangerous_sql_in_comment() -> None:
    engine = SqlSafetyEngine()
    decision = engine.evaluate("SELECT 1 /* DROP TABLE users */")
    assert not decision.allowed
    assert "Dangerous SQL found in comments" in decision.reasons[0]


def test_auto_adds_partition_filter_for_partitioned_table() -> None:
    engine = SqlSafetyEngine()
    decision = engine.evaluate("SELECT DATE(created_at) AS day, COUNT(*) AS c FROM analytics.events GROUP BY 1")
    assert decision.allowed
    assert decision.rewritten_sql is not None
    assert "DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)" in decision.rewritten_sql
