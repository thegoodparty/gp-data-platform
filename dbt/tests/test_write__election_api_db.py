"""Source-contract tests for the election-api dbt writer.

The writer drives its Postgres loads off parallel literal lists zipped
together inside model(); an edit that removes an entry from one list but not
its siblings would truncate (strict=False) or misalign the loads without any
error near the mistake. These tests pin the table set and the zip discipline
at the source level: the module only runs on a Databricks cluster (dbutils,
JDBC), and the lists live inside model(), so the contract is asserted by
parsing the source rather than importing and executing it.
"""

import ast
from pathlib import Path

WRITER_PATH = Path(__file__).parent.parent / "project" / "models" / "write" / "write__election_api_db.py"

# The writer's full table set, in FK-dependency order (parents before
# children); Projected_Turnout is intentionally absent — it is delivered by
# the sync_election_api Airflow DAG's atomic table swap (DATA-2015).
EXPECTED_LOAD_TABLES = ["Place", "District", "Position", "Race", "Candidacy", "Issue", "Stance"]

# The incremental max-updated_at filter covers every load table except
# Position, whose Postgres table has no updated_at column.
EXPECTED_INCREMENTAL_TABLES = ["Candidacy", "Issue", "Place", "Race", "Stance", "District"]


def _writer_tree() -> ast.Module:
    return ast.parse(WRITER_PATH.read_text())


def _zip_calls(tree: ast.Module) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "zip"
    ]


def _literal_string_list(node: ast.expr) -> list[str] | None:
    """The list of string constants in a list literal, or None if not one."""
    if not isinstance(node, ast.List):
        return None
    values = [elt.value for elt in node.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)]
    return values if len(values) == len(node.elts) else None


def _name_list(node: ast.expr) -> list[str] | None:
    """The identifier names in a list literal, or None if not one."""
    if not isinstance(node, ast.List):
        return None
    names = [elt.id for elt in node.elts if isinstance(elt, ast.Name)]
    return names if len(names) == len(node.elts) else None


def _zip_by_first_list(tree: ast.Module, table_names: list[str]) -> ast.Call:
    for call in _zip_calls(tree):
        if call.args and _literal_string_list(call.args[0]) == table_names:
            return call
    raise AssertionError(
        f"No zip() whose first argument is the literal table list {table_names}; "
        "if the writer was restructured, update these pins deliberately."
    )


def test_every_zip_is_strict():
    """A silently truncating zip over the parallel load lists must be
    impossible: every zip in the writer carries strict=True."""
    zips = _zip_calls(_writer_tree())
    assert len(zips) >= 2, "expected the incremental-filter zip and the load-loop zip"
    for call in zips:
        strict = [kw for kw in call.keywords if kw.arg == "strict"]
        assert strict, f"zip() at line {call.lineno} has no strict= keyword"
        assert (
            isinstance(strict[0].value, ast.Constant) and strict[0].value.value is True
        ), f"zip() at line {call.lineno} must use strict=True"


def test_load_loop_table_set_is_pinned():
    """The load loop writes exactly the seven tables, in FK-dependency
    order, with each table's DataFrame and upsert query aligned by name."""
    call = _zip_by_first_list(_writer_tree(), EXPECTED_LOAD_TABLES)
    assert len(call.args) == 3, "load loop zips (tables, dataframes, upsert queries)"

    dfs = _name_list(call.args[1])
    queries = _name_list(call.args[2])
    assert dfs is not None and queries is not None
    # Same length as the table list (strict=True would also catch this at
    # runtime, but only on a Databricks cluster; assert it here first).
    assert len(dfs) == len(EXPECTED_LOAD_TABLES)
    assert len(queries) == len(EXPECTED_LOAD_TABLES)
    # Positional alignment by naming convention: <table>_df / <TABLE>_UPSERT_QUERY.
    assert dfs == [f"{t.lower()}_df" for t in EXPECTED_LOAD_TABLES]
    assert queries == [f"{t.upper()}_UPSERT_QUERY" for t in EXPECTED_LOAD_TABLES]


def test_incremental_filter_table_set_is_pinned():
    """The incremental max-updated_at zip stays in lockstep with the load
    set (minus Position, which has no updated_at column)."""
    call = _zip_by_first_list(_writer_tree(), EXPECTED_INCREMENTAL_TABLES)
    assert len(call.args) == 2, "incremental filter zips (tables, dataframes)"

    dfs = _name_list(call.args[1])
    assert dfs is not None
    assert dfs == [f"{t.lower()}_df" for t in EXPECTED_INCREMENTAL_TABLES]
    assert set(EXPECTED_INCREMENTAL_TABLES) == set(EXPECTED_LOAD_TABLES) - {"Position"}


def test_projected_turnout_is_absent():
    """Projected_Turnout must never reappear in this writer: the upsert path
    cannot delete superseded rows, which is why its delivery moved to the
    sync_election_api swap DAG (DATA-2015). Checks every string literal, so
    a reintroduced table-list entry or upsert query both fail."""
    for node in ast.walk(_writer_tree()):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert "Projected_Turnout" not in node.value, (
                f"'Projected_Turnout' found in a string literal at line {node.lineno}; "
                "it is delivered by the sync_election_api swap DAG, not this writer."
            )
