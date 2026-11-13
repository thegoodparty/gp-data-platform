"""Tests for the Bifrost export -> warehouse contract.

The export itself (RDS -> one Parquet file per run in S3) lives in `thegoodparty/ops`, so what is
testable here is the contract around it: the projection, the COPY INTO statement, and the fact
that idempotency is left switched on. Everything is mocked; nothing reaches S3 or the warehouse.
"""

import sys
from unittest.mock import MagicMock

import pytest

# Stub the runtime modules so this collects in the airflow env, the dbt env or bare, matching
# test_postgres_utils.py. Direct assignment (not setdefault): the dbt env ships an
# incompatible airflow.sdk.
_STUBS = (
    "airflow",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from include.custom_functions.bifrost_export import (  # noqa: E402
    EXPORTED_COLUMNS,
    LANDING_COLUMNS,
    TABLE_NAME,
    BifrostRow,
    build_copy_sql,
    ensure_landing_table,
    export_prefix,
    land_export,
    rows_inserted,
)

# ---------------------------------------------------------------------------
# The projection is the contract
# ---------------------------------------------------------------------------


def test_projection_is_the_30_columns_the_export_writes_plus_loaded_at():
    """A column added to the loader but not the export lands as nulls; the reverse silently drops
    data. Pinning the count is deliberate: this is a cross-repo contract, and changing it is a
    change to what ops must write."""
    assert len(LANDING_COLUMNS) == 31  # 30 exported + loaded_at, which the loader stamps
    assert len(EXPORTED_COLUMNS) == 30


def test_exported_at_is_carried_and_is_not_server_set():
    """The export stamps every row with when it ran. That is what makes "which observation is
    current" answerable, so it has to survive the landing rather than be regenerated here."""
    names = [name for name, _ in EXPORTED_COLUMNS]
    assert "exported_at" in names
    assert "loaded_at" not in names


def test_projection_carries_no_payload_columns():
    """Content logging is off in the gateway today, so these are empty. They are excluded so a
    change to that setting cannot start shipping prompts and completions into the warehouse."""
    payload_columns = {
        "input_history",
        "responses_input_history",
        "output_message",
        "responses_output",
        "embedding_input",
        "embedding_output",
        "ocr_output",
        "raw_request",
        "raw_response",
        "passthrough_request_body",
        "passthrough_response_body",
        "content_summary",
        "token_usage",
        "tools",
        "tool_calls",
        "params",
        "error_details",
        "cache_debug",
        "guardrail_debug",
        "routing_metadata",
    }
    assert payload_columns.isdisjoint({name for name, _ in LANDING_COLUMNS})


def test_row_named_tuple_tracks_the_exported_columns_in_order():
    """A field that stopped lining up with its column would bind every later value one position
    off. The module raises at import if they disagree; this is the readable assertion."""
    assert BifrostRow._fields == tuple(name for name, _ in EXPORTED_COLUMNS)


def test_money_columns_are_decimal_not_double():
    """These are summed across hundreds of thousands of rows in the mart."""
    types = dict(LANDING_COLUMNS)
    for column in ("cost", "input_cost", "output_cost", "additional_cost"):
        assert types[column].startswith("DECIMAL"), f"{column} must be DECIMAL, not {types[column]}"
    assert types["latency"] == "DOUBLE"  # a measurement, not money


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def _fake_connection():
    """A connection whose cursor is a real MagicMock, context-managed the way the loader uses it.

    `closing(cursor)` calls `cursor.close()`, not `__exit__`, so the mock has to come back from
    `connection.cursor()` itself rather than from a context manager on it.
    """
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    return connection, cursor


def test_ensure_landing_table_creates_schema_then_table_idempotently():
    connection, cursor = _fake_connection()
    ensure_landing_table(connection, "catalog", "airflow_source")

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == "CREATE SCHEMA IF NOT EXISTS `catalog`.`airflow_source`"
    assert statements[1].startswith(f"CREATE TABLE IF NOT EXISTS `catalog`.`airflow_source`.`{TABLE_NAME}` (")
    assert "IF NOT EXISTS" in statements[1]
    # Reserved words must be quoted; `timestamp` and `alias` are both column names here.
    assert "`timestamp` TIMESTAMP" in statements[1]
    assert "`alias` STRING" in statements[1]


def test_ensure_landing_table_rejects_an_identifier_that_could_carry_sql():
    connection, _ = _fake_connection()
    with pytest.raises(ValueError):
        ensure_landing_table(connection, "catalog", "airflow_source`; drop table x; --")


# ---------------------------------------------------------------------------
# COPY INTO
# ---------------------------------------------------------------------------


def test_copy_into_leaves_idempotency_switched_on():
    """`COPY_OPTIONS ('force' = 'true')` disables idempotency and would reload every file on
    every run. It is the one option here that must stay absent."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")

    assert "force" not in sql.lower()
    assert "copy_options" not in sql.lower()


def test_copy_into_names_every_column_and_stamps_loaded_at_server_side():
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")

    target = sql.split("(", 1)[1].split(")", 1)[0]
    for column, _ in LANDING_COLUMNS:
        assert f"`{column}`" in target
    assert "current_timestamp() as `loaded_at`" in sql


def test_copy_into_target_and_projection_have_the_same_arity():
    """They must be equal or the statement fails on every run, which is exactly what an earlier
    version of this module did: 29 targets against 30 expressions, because `loaded_at` was dropped
    from the target and kept in the projection."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")

    target = sql.split("(", 1)[1].split(")", 1)[0]
    n_target = len([part for part in target.split(",") if part.strip()])

    projection = sql.split("from (select ", 1)[1].split("\nfileformat", 1)[0]
    depth, n_select = 0, 1
    for char in projection:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            n_select += 1

    assert n_target == n_select == len(LANDING_COLUMNS)


def test_copy_into_quotes_the_prefix_as_a_validated_literal():
    """The COPY INTO source position takes a location, not a bound parameter, so the path is
    validated and then quoted. The validation is the guard: it is what stops a prefix ending the
    literal or starting a second statement."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")
    assert "from 's3://b/p'" in sql


@pytest.mark.parametrize(
    "path",
    [
        "s3://b/p'",
        "s3://b/p; drop table x",
        "s3://b/p\n",
        "s3://b/p\r\n",
        's3://b/p"',
        "s3://b/p\x00",
        "http://bucket/key",
        "s3://bucket",
        "s3://bucket/",
        # Cannot escape the literal, but would point COPY INTO somewhere other than the prefix the
        # Variable names. Rejected rather than normalised.
        "s3://bucket/a/../etc",
        "s3://bucket/a//b",
        "s3://bucket/./x",
        "",
    ],
)
def test_copy_into_refuses_a_prefix_that_could_escape_the_literal(path):
    with pytest.raises(ValueError):
        build_copy_sql("catalog", "airflow_source", path)


def test_copy_into_does_not_qualify_the_projection_with_an_aliased_table():
    """The subquery has no alias, and `source.id` against an unaliased subquery is a hard
    UNRESOLVED_COLUMN in this warehouse (verified against it). Every one of the 30 cast
    expressions would fail, so nothing would ever land."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")

    assert "source." not in sql
    assert "as source" not in sql


def test_copy_into_never_selects_star():
    """`select *` would defeat the projection the moment the export's schema widened."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p")
    assert "select *" not in sql.lower()


def test_copy_into_declares_the_parquet_format():
    assert "fileformat = parquet" in build_copy_sql("catalog", "airflow_source", "s3://b/p").lower()


def test_land_export_binds_the_prefix_and_returns_the_row_count():
    connection, cursor = _fake_connection()
    cursor.fetchall.return_value = [("part-1.parquet", "SUCCESS", 12)]
    cursor.description = [
        ("file", "STRING", None, None, None, None, None),
        ("status", "STRING", None, None, None, None, None),
        ("rows_inserted", "BIGINT", None, None, None, None, None),
    ]

    loaded = land_export(
        connection,
        catalog="catalog",
        schema="airflow_source",
        bucket="bucket",
        prefix="bifrost/logs/",
    )

    assert loaded == 12
    # `execute_with_retry` always passes the parameters argument, even as None.
    statement, parameters = cursor.execute.call_args.args
    assert parameters is None
    assert "s3://bucket/bifrost/logs" in statement
    assert "copy into" in statement.lower()


@pytest.mark.parametrize(
    "columns, rows, expected",
    [
        # The documented shape: one row per file, the count in `rows_inserted`.
        (["file", "status", "rows_inserted"], [("a", "S", 5), ("b", "S", 7)], 12),
        # A re-run that skipped everything: files present, zero rows each.
        (["file", "rows_inserted"], [("a", 0), ("b", 0)], 0),
        # Nothing selected at all.
        (["file", "rows_inserted"], [], 0),
        # A count column under the other documented spelling.
        (["num_inserted_rows"], [(3,)], 3),
        # A null count is not a crash.
        (["rows_inserted"], [(None,), (4,)], 4),
        # Unknown shape: a changed contract, so it raises rather than reporting "no new rows".
        (["file", "status"], [("a", "S")], None),
    ],
)
def test_rows_inserted_reads_the_count_by_name_not_by_position(columns, rows, expected):
    """COPY INTO's result set carries `num_skipped_corrupt_files` and others besides the count, so
    hard-coding a position against a service that may reorder them is not safe."""
    if expected is None:
        with pytest.raises(ValueError):
            rows_inserted(rows, columns)
    else:
        assert rows_inserted(rows, columns) == expected


# ---------------------------------------------------------------------------
# The prefix
# ---------------------------------------------------------------------------


def test_export_prefix_normalises_slashes():
    assert export_prefix("bucket", "bifrost/logs/") == "s3://bucket/bifrost/logs"
    assert export_prefix("bucket", "/bifrost/logs") == "s3://bucket/bifrost/logs"


def test_export_prefix_refuses_an_empty_prefix():
    """An empty prefix would point COPY INTO at the bucket root."""
    with pytest.raises(ValueError):
        export_prefix("bucket", "   ")


# The DAG's own behaviour is tested in tests/dags/test_load_bifrost_usage_dag.py, where real
# Airflow is importable: grepping the file's source would also pass if the DAG did nothing.
