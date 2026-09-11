from datetime import UTC, date, datetime
from decimal import Decimal

from loader.databricks_writer import sql_literal


def test_none_is_null():
    assert sql_literal(None) == "NULL"


def test_bool():
    assert sql_literal(True) == "TRUE"
    assert sql_literal(False) == "FALSE"


def test_numbers():
    assert sql_literal(42) == "42"
    assert sql_literal(3.5) == "3.5"


def test_decimal_is_a_plain_numeric_literal():
    # Must render as e.g. "412.800000", never quoted and never in scientific notation
    # (Decimal's own str()/repr() can use "E+n" notation, which isn't valid numeric SQL).
    assert sql_literal(Decimal("412.800000")) == "412.800000"
    assert sql_literal(Decimal("0.00000005")) == "0.00000005"


def test_datetime_and_date():
    dt = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    assert sql_literal(dt) == f"TIMESTAMP '{dt.isoformat()}'"
    assert sql_literal(date(2026, 1, 1)) == "DATE '2026-01-01'"


def test_plain_string():
    assert sql_literal("hello") == "'hello'"


def test_single_quote_is_escaped():
    # A user name like O'Brien must not close the string literal early.
    assert sql_literal("O'Brien") == "'O\\'Brien'"


def test_backslash_is_escaped():
    assert sql_literal("a\\b") == "'a\\\\b'"


def test_sql_injection_attempt_is_neutralized():
    malicious = "x'); DROP TABLE users; --"
    literal = sql_literal(malicious)
    # The escaped literal must still be a single quoted string with no unescaped quote in it.
    assert literal.startswith("'") and literal.endswith("'")
    assert literal.count("\\'") == literal.count("'") - 2  # every embedded quote is escaped
