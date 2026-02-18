"""Phase 5 tests: NL to SQL, SQL validation, safety checks."""

from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.models import (
    ColumnProfile,
    SchemaProfile,
    TableProfile,
    ThinContract,
)
from data_autopilot.services.mode1.nl_to_sql import NLToSQL
from data_autopilot.services.mode1.sql_validator import (
    SQLValidator,
)


def _make_schema() -> SchemaProfile:
    return SchemaProfile(tables=[
        TableProfile(
            name="fct_orders",
            columns=[
                ColumnProfile(name="id", data_type="integer", is_primary_key=True),
                ColumnProfile(name="customer_id", data_type="integer"),
                ColumnProfile(name="amount", data_type="decimal"),
                ColumnProfile(name="refund_amount", data_type="decimal"),
                ColumnProfile(name="tax", data_type="decimal"),
                ColumnProfile(name="category", data_type="varchar"),
                ColumnProfile(name="created_at", data_type="timestamp"),
            ],
            row_count=145000,
            detected_keys=["id", "customer_id"],
            detected_time_columns=["created_at"],
        ),
        TableProfile(
            name="dim_users",
            columns=[
                ColumnProfile(name="id", data_type="integer", is_primary_key=True),
                ColumnProfile(name="email", data_type="varchar"),
                ColumnProfile(name="signup_date", data_type="date"),
            ],
            row_count=45000,
            detected_keys=["id"],
            detected_time_columns=["signup_date"],
        ),
    ])


def test_nl_to_sql_simple_count() -> None:
    """5.5: 'How many users signed up last week?' → valid SELECT COUNT query."""
    schema = _make_schema()
    aliases = EntityAliasManager()
    aliases.set_alias("org_1", "dim_users", "Users")

    nl = NLToSQL(alias_manager=aliases)
    result = nl.generate("How many users signed up last week?", schema, org_id="org_1")

    assert result.validated
    sql = result.sql.upper()
    assert "COUNT" in sql
    assert "DIM_USERS" in sql
    assert "LAST WEEK" not in sql  # Should be converted to interval


def test_nl_to_sql_aggregation() -> None:
    """5.6: 'Revenue by product category' → valid GROUP BY query."""
    schema = _make_schema()
    aliases = EntityAliasManager()
    aliases.set_alias("org_1", "fct_orders", "Orders")

    nl = NLToSQL(alias_manager=aliases)
    result = nl.generate("Total revenue by category", schema, org_id="org_1")

    assert result.validated
    sql = result.sql.upper()
    assert "FCT_ORDERS" in sql
    assert "GROUP BY" in sql
    assert "CATEGORY" in sql


def test_nl_to_sql_with_contract() -> None:
    """5.7: 'What's revenue?' with contract → SQL applies refund subtraction."""
    schema = _make_schema()
    contract = ThinContract(revenue_definition="net_after_refunds")

    nl = NLToSQL()
    result = nl.generate(
        "What's the total revenue?",
        schema,
        contract=contract,
    )

    assert result.validated
    sql = result.sql.upper()
    assert "FCT_ORDERS" in sql
    # Should contain refund subtraction
    assert "REFUND" in sql


def test_sql_safety_blocks_ddl() -> None:
    """5.8: LLM generates DROP TABLE → caught and blocked."""
    validator = SQLValidator()
    schema = _make_schema()

    result = validator.validate("DROP TABLE fct_orders", schema)
    assert not result.validated
    assert result.error is not None
    assert "SELECT" in result.error or "DROP" in result.error


def test_sql_safety_blocks_dml() -> None:
    """5.9: LLM generates INSERT INTO → caught and blocked."""
    validator = SQLValidator()
    schema = _make_schema()

    result = validator.validate("INSERT INTO fct_orders (id) VALUES (1)", schema)
    assert not result.validated
    assert result.error is not None


def test_sql_safety_adds_limit() -> None:
    """5.10: Query without LIMIT → LIMIT 10000 added."""
    validator = SQLValidator()
    schema = _make_schema()

    result = validator.validate("SELECT * FROM fct_orders", schema)
    assert result.validated
    assert "LIMIT" in result.sql.upper()
    assert "10000" in result.sql


def test_sql_safety_invalid_table() -> None:
    """5.11: Query references non-existent table → caught and blocked."""
    validator = SQLValidator()
    schema = _make_schema()

    result = validator.validate("SELECT * FROM nonexistent_table", schema)
    assert not result.validated
    assert "not found" in result.error.lower()
