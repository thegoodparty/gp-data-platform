"""Tests for the Bifrost export -> warehouse loader.

The export itself (RDS -> Parquet in S3) lives in `thegoodparty/ops`, so what is testable here
is the contract around it: the projection, the insert's anti-join, partition discovery, and the
window. Everything is mocked; nothing reaches S3 or the warehouse.
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
    INSERT_COLUMNS,
    LANDING_COLUMNS,
    TABLE_NAME,
    BifrostRow,
    build_insert_sql,
    days_in_window,
    ensure_landing_table,
    insert_day,
    partition_path,
    success_marker_path,
)

# ---------------------------------------------------------------------------
# The projection is the contract
# ---------------------------------------------------------------------------


def test_projection_is_the_29_columns_the_export_writes():
    """A column added to the loader but not the export lands as nulls; the reverse silently
    drops data. Pinning the count is intentional here: this is a cross-repo contract, and
    changing it is a change to what ops must write."""
    assert len(LANDING_COLUMNS) == 30  # 29 exported + loaded_at, which the loader stamps
    exported = {name for name, _ in LANDING_COLUMNS} - {"loaded_at"}
    assert len(exported) == 29


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


def test_row_named_tuple_tracks_the_columns_in_order():
    """A field that stopped lining up with its column would bind every later value one position
    off. The module raises at import if they disagree; this is the readable assertion."""
    assert BifrostRow._fields == INSERT_COLUMNS


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
# INSERT
# ---------------------------------------------------------------------------


def test_insert_is_guarded_by_a_null_safe_anti_join():
    """The window is re-emitted every run, so unchanged rows must be filtered by (id, status,
    cost). `<=>` rather than `=` is load-bearing: a plain `=` never matches NULL, so every
    unpriced `processing` row would be re-inserted on every single run."""
    sql = build_insert_sql("catalog", "airflow_source", "s3://b/p/dt=2026-11-04")

    assert "where not exists" in sql
    assert "landed.`id` = source.`id`" in sql
    assert "landed.`status` <=> source.`status`" in sql
    assert "landed.`cost` <=> source.`cost`" in sql
    assert "landed.`cost` = source.`cost`" not in sql


def test_insert_names_every_column_and_stamps_loaded_at_server_side():
    sql = build_insert_sql("catalog", "airflow_source", "s3://b/p/dt=2026-11-04")

    insert_columns = sql.split("(", 1)[1].split(")", 1)[0]
    for column in INSERT_COLUMNS:
        assert f"`{column}`" in insert_columns
    assert "`loaded_at`" not in insert_columns
    assert "current_timestamp() as `loaded_at`" in sql


def test_insert_never_selects_star():
    """`select *` would defeat the projection the moment the export's schema widened."""
    sql = build_insert_sql("catalog", "airflow_source", "s3://b/p/dt=2026-11-04")
    assert "select *" not in sql.lower()


def test_insert_binds_the_path_rather_than_interpolating_it():
    """The path is a parameter; only the identifiers are interpolated, and they are validated."""
    sql = build_insert_sql("catalog", "airflow_source", "s3://b/p/dt=2026-11-04")
    assert ":path" in sql
    assert "s3://b/p/dt=2026-11-04" not in sql


def test_insert_passes_an_explicit_parquet_schema():
    """With no schema, a widened upstream type lands a column of nulls instead of failing."""
    sql = build_insert_sql("catalog", "airflow_source", "s3://b/p/dt=2026-11-04")
    assert "format => 'parquet'" in sql
    assert "schema => '`id` STRING" in sql


def test_insert_day_returns_the_row_count_and_binds_the_partition_path():
    connection, cursor = _fake_connection()
    cursor.fetchone.return_value = (7,)

    inserted = insert_day(
        connection,
        catalog="catalog",
        schema="airflow_source",
        bucket="bucket",
        prefix="bifrost/logs/",
        partition_day="2026-11-04",
    )

    assert inserted == 7
    statement, parameters = cursor.execute.call_args.args
    assert parameters == {"path": "s3://bucket/bifrost/logs/dt=2026-11-04"}
    assert ":path" in statement


def test_insert_day_reports_zero_when_the_cursor_returns_nothing():
    connection, cursor = _fake_connection()
    cursor.fetchone.return_value = None
    assert (
        insert_day(
            connection,
            catalog="catalog",
            schema="airflow_source",
            bucket="bucket",
            prefix="p",
            partition_day="2026-11-04",
        )
        == 0
    )


# ---------------------------------------------------------------------------
# Paths and markers
# ---------------------------------------------------------------------------


def test_partition_path_normalises_the_prefix():
    assert partition_path("bucket", "bifrost/logs/", "2026-11-04") == "s3://bucket/bifrost/logs/dt=2026-11-04"
    assert partition_path("bucket", "/bifrost/logs", "2026-11-04") == "s3://bucket/bifrost/logs/dt=2026-11-04"


def test_partition_path_refuses_a_day_that_is_not_an_iso_date():
    """The day is interpolated into a path that reaches `read_files`, so it is validated."""
    for bad in ("2026-11-4", "latest", "dt=2026-11-04", "", "2026-11-04/../x"):
        with pytest.raises(ValueError):
            partition_path("bucket", "p", bad)


def test_success_marker_lives_inside_the_day_partition():
    assert success_marker_path("bucket", "p", "2026-11-04") == "s3://bucket/p/dt=2026-11-04/_SUCCESS"


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------


def test_window_keeps_every_day_inside_it_including_days_already_landed():
    """The regression for the plan's critical defect. A watermark on "newest landed" would drop
    a re-emitted day, so a request first seen `processing` would never be re-read."""
    days = ["2026-11-04", "2026-11-03", "2026-11-02", "2026-10-01"]

    selected = days_in_window(days, lookback_days=3, today="2026-11-04")

    assert selected == ["2026-11-04", "2026-11-03", "2026-11-02"]


def test_window_is_inclusive_of_the_oldest_day_in_it():
    assert days_in_window(["2026-11-02"], lookback_days=3, today="2026-11-04") == ["2026-11-02"]
    assert days_in_window(["2026-11-01"], lookback_days=3, today="2026-11-04") == []


def test_full_reload_keeps_everything_regardless_of_the_window():
    days = ["2026-11-04", "2026-09-01"]
    assert days_in_window(days, lookback_days=3, today="2026-11-04", full_reload=True) == [
        "2026-11-04",
        "2026-09-01",
    ]


def test_window_drops_future_and_malformed_days():
    days = ["2026-11-05", "2026-11-04", "2026-11-4", "junk", ""]
    assert days_in_window(days, lookback_days=7, today="2026-11-04") == ["2026-11-04"]


def test_window_rejects_a_nonsense_lookback():
    with pytest.raises(ValueError):
        days_in_window([], lookback_days=0, today="2026-11-04")


# ---------------------------------------------------------------------------
# The DAG's own structure
# ---------------------------------------------------------------------------


def _load_dag_module():
    """Load the DAG file directly, the way test_gold_match_daily_dag.py does: CI does not
    point dags_folder at astro/dags.

    The DAG module imports `airflow.sdk` for real (dag/task/Variable/Param), so this test only
    runs where the Astro runtime or the airflow uv env is installed. Where the stubs above are
    in force it is skipped rather than failing on a MagicMock dag decorator.
    """
    from pathlib import Path

    dag_file = Path(__file__).resolve().parents[1] / "dags" / "load_bifrost_usage.py"
    source = dag_file.read_text()
    return dag_file, source


def test_dag_file_declares_the_structure_the_plan_relies_on():
    """Asserted from the source rather than by importing, so it runs under the stubs."""
    dag_file, source = _load_dag_module()

    assert dag_file.is_file()
    # Serialised writers: the repo's uniform guard against two runs colliding.
    assert "max_active_runs=1" in source
    # BUILD must not schedule anything; unpausing is the owner-gated activation step.
    assert "is_paused_upon_creation=True" in source
    # The landing table has to exist before the first insert runs.
    assert "ensure_landing_table(connection, catalog, schema)" in source
    assert source.index("ensure_landing_table(") < source.index("insert_day(")
    # Nothing here deletes: pruning would make the mart's history for a day diverge.
    assert ".delete(" not in source.lower()
    assert "delete from" not in source.lower()


def test_dag_creates_the_landing_table_from_the_loader_constant():
    """The DDL and the INSERT read one source of truth, so they cannot drift apart."""
    _, source = _load_dag_module()
    assert "from include.custom_functions.bifrost_export import" in source
    assert "ensure_landing_table" in source
    # No second spelling of the table name or its columns in the DAG.
    assert "bifrost_request_log" not in source.split('"""', 2)[2]


def test_ensure_landing_table_matches_the_one_time_ddl_in_the_ticket_notes():
    """The ticket's one-time DDL is the same schema, hand-run before the models merge. If the
    constant moves and the notes do not, whichever landed second wins silently and the two
    disagree about a column's type. Cheap to pin, and it caught the projection being 29 columns
    rather than 28."""
    from pathlib import Path

    notes = Path("/work/tickets/gp-data-platform/DATA-2808/plan.md")
    if not notes.is_file():
        pytest.skip("ticket notes are outside the repo; only present in the working sandbox")

    text = notes.read_text()
    marker = "CREATE TABLE IF NOT EXISTS goodparty_data_catalog.airflow_source.bifrost_request_log ("
    assert marker in text, "the one-time DDL is missing from the plan"
    block = text.split(marker, 1)[1].split(") CLUSTER BY AUTO;", 1)[0]

    columns = []
    for line in block.strip().split("\n"):
        line = line.strip().rstrip(",")
        name, _, column_type = line.partition(" ")
        columns.append((name.strip("`"), column_type.strip()))

    assert columns == list(LANDING_COLUMNS)
