"""Tests for the delegate swarm export -> warehouse contract.

The export itself (SQLite -> two JSON Lines files per run in S3) lives in `thegoodparty/ops`, so what
is testable here is the contract around it: the two projections, the two COPY INTO statements, the
row types that mirror them, and the fact that idempotency is left switched on. Everything is mocked;
nothing reaches S3 or the warehouse.
"""

import sys
from unittest.mock import MagicMock

import pytest

# Stub the runtime modules that are not importable, so this collects in the airflow env, the dbt env
# or bare. Only the genuinely missing ones are stubbed: replacing a module that is present poisons
# `sys.modules` for every later test file in the same process, and the DAG suites import real Airflow
# at collection time (which is exactly what makes them worth running).
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
    try:
        __import__(_mod)
    except ImportError:
        sys.modules[_mod] = MagicMock()

from include.custom_functions.delegate_swarm_export import (  # noqa: E402
    SESSION_COST,
    SESSION_COST_EXPORTED_COLUMNS,
    SESSION_COST_LANDING_COLUMNS,
    SESSION_COST_TABLE_NAME,
    TABLES,
    TASK_EDGE,
    TASK_EDGE_EXPORTED_COLUMNS,
    TASK_EDGE_LANDING_COLUMNS,
    TASK_EDGE_TABLE_NAME,
    Export,
    LandingTable,
    SessionCostRow,
    TaskEdgeRow,
    build_copy_sql,
    check_row_types,
    ensure_landing_table,
    export_prefix,
    land_export,
    land_exports,
    plan_landings,
)

# ---------------------------------------------------------------------------
# The cross-repo column pin
# ---------------------------------------------------------------------------

# The exporter's serializer keys, copied here as literals from the ops side
# (`agent-swarm/host/export-usage-core.ts`, `SESSION_COST_COLUMNS` and `TASK_EDGE_COLUMNS`).
#
# A checked-in copy, not an import: this repo cannot read the ops repo at build time and must not
# depend on it at run time. So this pin fails on a local edit to either side's list, which is the
# earliest signal available, and keeping the two copies in step is a gap a human closes. The PR that
# changes one list states the other in its description.
_PINNED_SESSION_COST_COLUMNS = (
    "id",
    "session_id",
    "task_id",
    "agent_id",
    "agent_name",
    "model",
    "cost_source",
    "kind",
    "total_cost_usd",
    "harness_cost_usd",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "cache_write_5m_tokens",
    "cache_write_1h_tokens",
    "reasoning_output_tokens",
    "thinking_tokens",
    "duration_ms",
    "num_turns",
    "is_error",
    "created_at",
    "exported_at",
)

_PINNED_TASK_EDGE_COLUMNS = (
    "task_id",
    "parent_task_id",
    "dispatched_by_task_id",
    "status",
    "created_at",
    "exported_at",
)


def test_the_exported_cost_columns_are_the_exporters_serializer_keys_in_order():
    """The export writes exactly these keys. A column added on one side and not the other lands as
    nulls, and the order matters because the exporter's `toLandingRow` builds its objects from its
    own list."""
    assert tuple(name for name, _ in SESSION_COST_EXPORTED_COLUMNS) == _PINNED_SESSION_COST_COLUMNS


def test_the_exported_edge_columns_are_the_exporters_serializer_keys_in_order():
    assert tuple(name for name, _ in TASK_EDGE_EXPORTED_COLUMNS) == _PINNED_TASK_EDGE_COLUMNS


def test_the_projection_counts_are_the_contract_plus_loaded_at():
    """23 cost columns and 6 edge columns from `02-spec.md` section 1, plus `loaded_at` which this
    loader stamps. Pinning the counts is deliberate: this is a cross-repo contract, and changing it is
    a change to what ops must write."""
    assert len(SESSION_COST_LANDING_COLUMNS) == 24  # 23 exported + loaded_at
    assert len(SESSION_COST_EXPORTED_COLUMNS) == 23
    assert len(TASK_EDGE_LANDING_COLUMNS) == 7  # 6 exported + loaded_at
    assert len(TASK_EDGE_EXPORTED_COLUMNS) == 6


def test_exported_at_is_carried_and_is_not_server_set():
    """The export stamps every row with when it ran. That is what makes "which observation is
    current" answerable, so it has to survive the landing rather than be regenerated here."""
    for table in TABLES:
        names = [name for name, _ in table.exported_columns]
        assert "exported_at" in names, table.name
        assert "loaded_at" not in names, table.name


def test_row_types_track_the_exported_columns_in_order():
    """A field that stopped lining up with its column would bind every later value one position off.
    The module raises at import if they disagree; these are the readable assertions."""
    assert SessionCostRow._fields == tuple(name for name, _ in SESSION_COST_EXPORTED_COLUMNS)
    assert TaskEdgeRow._fields == tuple(name for name, _ in TASK_EDGE_EXPORTED_COLUMNS)


def test_the_module_refuses_a_row_type_out_of_step_with_its_columns():
    """The import-time check is the guard, and a test can only prove it by driving it: a local edit
    that adds a column without its field has to fail loudly, not land values one position off.

    This is the red-cheap test the task asks for. Add a column to the tuple below and this fails;
    leave it as it is and the check passes, which is what the module's own import did.
    """
    shifted = (*TASK_EDGE.exported_columns, ("a_new_column", "STRING"))
    bad_table = LandingTable(TASK_EDGE_TABLE_NAME, shifted, TaskEdgeRow)
    with pytest.raises(RuntimeError, match="a_new_column"):
        check_row_types((bad_table,))
    # And the real tables pass it, which is what the import already ran.
    check_row_types(TABLES)


def test_the_payload_columns_are_absent():
    """The contract exports no task text, no prompt, no output, no failure or rejection reason, and
    no credential fingerprint. This is the list of what must never appear, so a widening of the
    export cannot quietly carry content into the warehouse."""
    forbidden = {
        # every free text column in `agent_tasks`
        "task",
        "output",
        "progress",
        "failure_reason",
        "rejection_reason",
        # a key fingerprint, no reporting need
        "credential_key_type",
        "credential_key_suffix",
        # an upstream-owned JSON array; today's sessions each have one model, in `model`
        "model_breakdown",
    }
    for table in TABLES:
        assert forbidden.isdisjoint({name for name, _ in table.columns}), table.name


def test_money_columns_are_decimal_not_double():
    """These are summed across every session in the mart."""
    types = dict(SESSION_COST_LANDING_COLUMNS)
    for column in ("total_cost_usd", "harness_cost_usd"):
        assert types[column] == "DECIMAL(20,6)"


def test_the_numeric_columns_are_integers_and_timestamps_are_timestamps():
    """A money string cast to STRING would sort lexically in the staging model, and an integer landed
    as a double is a token count with a rounding error."""
    types = dict(SESSION_COST_LANDING_COLUMNS)
    for column in (
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "reasoning_output_tokens",
        "thinking_tokens",
        "duration_ms",
        "num_turns",
    ):
        assert types[column] == "BIGINT", column
    # A 0/1 flag, not a count: landed as the integer the ledger wrote, so the staging model is the
    # one place that decides what a failed session looks like.
    assert types["is_error"] == "INT"
    for column in ("created_at", "exported_at", "loaded_at"):
        assert types[column] == "TIMESTAMP", column


def test_the_two_tables_have_distinct_names_and_keys():
    """Two tables rather than one with a record-kind column, because the two grains have different
    keys. If these collapsed to one name every landing would be one table's."""
    assert SESSION_COST_TABLE_NAME != TASK_EDGE_TABLE_NAME
    assert SESSION_COST.name == SESSION_COST_TABLE_NAME
    assert TASK_EDGE.name == TASK_EDGE_TABLE_NAME


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


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_ensure_landing_table_creates_schema_then_table_idempotently(table):
    connection, cursor = _fake_connection()
    ensure_landing_table(connection, "catalog", "airflow_source", table)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == "CREATE SCHEMA IF NOT EXISTS `catalog`.`airflow_source`"
    assert statements[1].startswith(f"CREATE TABLE IF NOT EXISTS `catalog`.`airflow_source`.`{table.name}` (")
    assert "IF NOT EXISTS" in statements[1]
    for name, column_type in table.columns:
        assert f"`{name}` {column_type}" in statements[1], f"{table.name}.{name}"


def test_ensure_landing_table_quotes_every_column_name():
    """Identifiers are backtick-quoted so a name that collides with a reserved word is safe without a
    per-name list, and so the DDL is the same shape for every column."""
    connection, cursor = _fake_connection()
    ensure_landing_table(connection, "catalog", "airflow_source", TASK_EDGE)
    statement = cursor.execute.call_args_list[1].args[0]
    assert "`status` STRING" in statement
    assert "`task_id` STRING" in statement


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_ensure_landing_table_rejects_an_identifier_that_could_carry_sql(table):
    connection, _ = _fake_connection()
    with pytest.raises(ValueError):
        ensure_landing_table(connection, "catalog", "airflow_source`; drop table x; --", table)


# ---------------------------------------------------------------------------
# COPY INTO
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_leaves_idempotency_switched_on(table):
    """`COPY_OPTIONS ('force' = 'true')` disables idempotency and would reload every file on every
    run. It is the one option here that must stay absent."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)

    assert "force" not in sql.lower()
    assert "copy_options" not in sql.lower()


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_carries_no_target_column_list(table):
    """Without a target list the mapping is by name, and the projection's aliases are the whole
    mapping. A target list would be a second copy of the same thing, and would make position matter
    again. The table name is the last thing before the newline that starts `from`, and no
    parenthesis may follow it on that line."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)

    first_line = sql.split("\n", 1)[0]
    assert first_line.lower().startswith("copy into ")
    assert "(" not in first_line and ")" not in first_line
    assert first_line.rstrip() == f"copy into `catalog`.`airflow_source`.`{table.name}`"


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_declares_json_and_no_schema_merge(table):
    """The files are JSON Lines, not Parquet, so the format is `json`. `mergeSchema` stays off: a
    file with a column the table lacks fails the load rather than growing the table."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)

    assert "fileformat = json" in sql.lower()
    assert "parquet" not in sql.lower()
    assert "mergeschema" not in sql.lower()
    assert "by position" not in sql.lower()
    # JSON Lines is one object per line, which is the format's default. `multiLine` on would expect
    # one array or one pretty-printed object per file and load nothing from these.
    assert "multiline" not in sql.lower()
    assert "format_options" not in sql.lower()


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_projection_aliases_every_landing_column_and_stamps_loaded_at(table):
    """With no target list the mapping is by name, so every expression's alias decides which column
    it fills: an expression that lost its `as <alias>` clause would leave that column null.
    `loaded_at` has no source column at all and is mapped only by its alias."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)

    assert "current_timestamp() as `loaded_at`" in sql
    for column, _ in table.exported_columns:
        assert f"as `{column}`" in sql, f"{table.name}: {column} is not aliased"


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_projection_carries_one_expression_per_column(table):
    """One expression per column, each aliased to its own name. Arity is the failure an earlier
    version of the Bifrost module shipped: 29 expressions against 30 targets, because `loaded_at` was
    dropped from the target list and kept in the projection."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)

    projection = sql.split("from (select ", 1)[1].split("\nfileformat", 1)[0]
    depth, n_select = 0, 1
    for char in projection:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            n_select += 1

    aliases = [f"as `{name}`" for name, _ in table.columns]
    assert n_select == len(table.columns)
    # Membership and count, not order: the mapping is by name, so position does not matter here.
    assert [alias for alias in aliases if alias in projection] == aliases
    for alias in aliases:
        assert projection.count(alias) == 1, f"{alias} appears {projection.count(alias)} times"


def test_the_two_statements_name_their_own_table_and_casts():
    """Two tables, two statements, and neither is the other's: a copy-paste that left one pointing
    at the cost table would land the edge file's columns into the cost table's names."""
    cost_sql = build_copy_sql("catalog", "airflow_source", "s3://b/cost", SESSION_COST)
    edge_sql = build_copy_sql("catalog", "airflow_source", "s3://b/edge", TASK_EDGE)

    assert SESSION_COST_TABLE_NAME in cost_sql and SESSION_COST_TABLE_NAME not in edge_sql
    assert TASK_EDGE_TABLE_NAME in edge_sql and TASK_EDGE_TABLE_NAME not in cost_sql
    assert "cast(`total_cost_usd` as DECIMAL(20,6)) as `total_cost_usd`" in cost_sql
    assert "total_cost_usd" not in edge_sql
    assert "cast(`status` as STRING) as `status`" in edge_sql
    assert "`status`" not in cost_sql


def test_copy_into_quotes_the_prefix_as_a_validated_literal():
    """The COPY INTO source position takes a location, not a bound parameter, so the path is
    validated and then quoted. The validation is the guard: it is what stops a prefix ending the
    literal or starting a second statement."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", SESSION_COST)
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
        build_copy_sql("catalog", "airflow_source", path, SESSION_COST)


def test_copy_into_does_not_qualify_the_projection_with_an_aliased_table():
    """The subquery has no alias, and `source.id` against an unaliased subquery is a hard
    UNRESOLVED_COLUMN in this warehouse. Every cast expression would fail, so nothing would land."""
    for table in TABLES:
        sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)
        assert "source." not in sql
        assert "as source" not in sql


@pytest.mark.parametrize("table", TABLES, ids=[table.name for table in TABLES])
def test_copy_into_never_selects_star(table):
    """`select *` would defeat the projection the moment the export's schema widened."""
    sql = build_copy_sql("catalog", "airflow_source", "s3://b/p", table)
    assert "select *" not in sql.lower()


# ---------------------------------------------------------------------------
# Landing
# ---------------------------------------------------------------------------


def test_land_export_quotes_the_prefix_and_returns_the_row_count():
    connection, cursor = _fake_connection()
    cursor.fetchall.return_value = [("cost-1.ndjson", "SUCCESS", 12)]
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
        prefix="delegate-swarm/session-cost/",
        table=SESSION_COST,
    )

    assert loaded == 12
    # `execute_with_retry` always passes the parameters argument, even as None.
    statement, parameters = cursor.execute.call_args.args
    assert parameters is None
    assert "s3://bucket/delegate-swarm/session-cost" in statement
    assert "copy into" in statement.lower()


def test_land_exports_creates_both_tables_before_landing_either():
    """A landing table that does not exist has to fail the run before any file is touched, not
    halfway through. Ordering is the whole content of this test, so the DDL and the COPY statements
    are recorded in one list as the cursor sees them."""
    events = []
    connection, cursor = _fake_connection()

    def record_execute(statement, *_args, **_kwargs):
        if statement.startswith("CREATE SCHEMA"):
            events.append(("create_schema", ""))
        elif statement.startswith("CREATE TABLE"):
            events.append(("create_table", statement.rsplit("`.`", 1)[1].split("`")[0]))
        else:
            events.append(("copy", statement.rsplit("`.`", 1)[1].split("`")[0]))
        cursor.description = [
            ("file", "STRING", None, None, None, None, None),
            ("status", "STRING", None, None, None, None, None),
            ("rows_inserted", "BIGINT", None, None, None, None, None),
        ]
        cursor.fetchall.return_value = [("file", "SUCCESS", 5)]

    cursor.execute.side_effect = record_execute

    counts = land_exports(
        connection,
        "catalog",
        "airflow_source",
        "bucket",
        (
            Export(SESSION_COST, "delegate-swarm/session-cost"),
            Export(TASK_EDGE, "delegate-swarm/task-edge"),
        ),
    )

    assert counts == {SESSION_COST_TABLE_NAME: 5, TASK_EDGE_TABLE_NAME: 5}
    # Both tables created before either prefix is read: a missing relation then fails the run before
    # any file is landed rather than between the two tables.
    assert [event[0] for event in events] == ["create_schema", "create_table"] * 2 + ["copy", "copy"]
    assert [event[1] for event in events if event[0] != "create_schema"] == [
        SESSION_COST_TABLE_NAME,
        TASK_EDGE_TABLE_NAME,
        SESSION_COST_TABLE_NAME,
        TASK_EDGE_TABLE_NAME,
    ]


def test_land_exports_lands_the_cost_file_first():
    """The export writes the cost file first and stops on a failure. Landing in the same order means a
    run that fails between the two leaves the cost rows, which the daily mart can read, and the edges
    arrive on the next run's window."""
    connection, cursor = _fake_connection()
    seen = []

    def record_execute(statement, *_args, **_kwargs):
        if statement.lower().startswith("copy into"):
            seen.append(statement.rsplit("`.`", 1)[1].split("`")[0])
        cursor.description = [
            ("file", "STRING", None, None, None, None, None),
            ("status", "STRING", None, None, None, None, None),
            ("rows_inserted", "BIGINT", None, None, None, None, None),
        ]
        cursor.fetchall.return_value = [("file", "SUCCESS", 0)]

    cursor.execute.side_effect = record_execute

    land_exports(
        connection,
        "catalog",
        "airflow_source",
        "bucket",
        (
            Export(SESSION_COST, "delegate-swarm/session-cost"),
            Export(TASK_EDGE, "delegate-swarm/task-edge"),
        ),
    )

    assert seen == [SESSION_COST_TABLE_NAME, TASK_EDGE_TABLE_NAME]


def test_a_bad_second_prefix_fails_before_the_first_table_is_touched():
    """Both statements are built before either is executed. A blank or malformed edge prefix is a
    Variable misconfiguration, and the next export window cannot heal one: without this, every run
    would land costs with no edges until an operator noticed, so the run has to fail before the cost
    COPY INTO commits anything."""
    connection, cursor = _fake_connection()

    with pytest.raises(ValueError):
        land_exports(
            connection,
            "catalog",
            "airflow_source",
            "bucket",
            (
                Export(SESSION_COST, "delegate-swarm/session-cost"),
                Export(TASK_EDGE, "   "),
            ),
        )

    cursor.execute.assert_not_called()


def test_plan_landings_builds_every_statement_up_front():
    plans = plan_landings(
        "catalog",
        "airflow_source",
        "bucket",
        (
            Export(SESSION_COST, "delegate-swarm/session-cost"),
            Export(TASK_EDGE, "delegate-swarm/task-edge"),
        ),
    )

    assert [plan.table.name for plan in plans] == [SESSION_COST_TABLE_NAME, TASK_EDGE_TABLE_NAME]
    assert [plan.path for plan in plans] == [
        "s3://bucket/delegate-swarm/session-cost",
        "s3://bucket/delegate-swarm/task-edge",
    ]
    for plan in plans:
        assert plan.table.name in plan.sql
        assert plan.path in plan.sql


# ---------------------------------------------------------------------------
# The prefix
# ---------------------------------------------------------------------------


def test_export_prefix_normalises_slashes():
    assert (
        export_prefix("bucket", "delegate-swarm/session-cost/") == "s3://bucket/delegate-swarm/session-cost"
    )
    assert (
        export_prefix("bucket", "/delegate-swarm/session-cost") == "s3://bucket/delegate-swarm/session-cost"
    )


def test_export_prefix_refuses_an_empty_prefix():
    """An empty prefix would point COPY INTO at the bucket root."""
    with pytest.raises(ValueError):
        export_prefix("bucket", "   ")


# The DAG's own behaviour is tested in tests/dags/test_load_delegate_swarm_usage_dag.py, where real
# Airflow is importable: grepping the file's source would also pass if the DAG did nothing.
