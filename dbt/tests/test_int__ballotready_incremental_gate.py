"""Source-contract tests for the BallotReady enrichment incremental gates.

The candidacy and person intermediates are incremental Python models that only
execute on a Databricks cluster, so the gate discipline is pinned by parsing
the source. Three properties matter: the incremental cutoff is the feed's
Airbyte ingest timestamp, never a vendor updated_at (vendor backfills deliver
files whose rows carry old vendor timestamps, which a vendor watermark skips
forever); a run against a table that predates the gate column refuses before
spending API budget; and each run's returned output lands the column the next
run's cutoff reads.
"""

import ast
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent / "project" / "models" / "intermediate" / "ballotready_api"

MODEL_NAMES = ("int__ballotready_candidacy", "int__ballotready_person")

# Vendor-clock columns that must never be part of an incremental gate again.
VENDOR_TIMESTAMP_COLUMNS = {"updated_at", "candidacy_updated_at", "race_updated_at"}

# Fail-fast guards required before any API fetch: the candidacy model checks
# its own table; the person model checks its own table plus the upstream
# candidacy relation whose gate column its filters dereference.
EXPECTED_GUARDS = {"int__ballotready_candidacy": 1, "int__ballotready_person": 2}

API_TOKEN_FACTORIES = {"_get_candidacy_token", "_get_person_token"}

ORDERING_OPS = (ast.Gt, ast.GtE, ast.Lt, ast.LtE)


def _tree(model_name: str) -> ast.Module:
    return ast.parse((MODELS_DIR / f"{model_name}.py").read_text())


def _model_function(tree: ast.Module) -> ast.FunctionDef:
    return next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "model")


def _watermark_agg_columns(tree: ast.Module) -> set[str]:
    """Columns read via the dict-style ``.agg({"col": "max"})`` cutoff pattern."""
    columns: set[str] = set()
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "agg"
            and node.args
            and isinstance(node.args[0], ast.Dict)
        ):
            continue
        for key, value in zip(node.args[0].keys, node.args[0].values, strict=True):
            if (
                isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and isinstance(value, ast.Constant)
                and value.value == "max"
            ):
                columns.add(key.value)
    return columns


def _referenced_columns(node: ast.AST) -> set[str]:
    """Column names referenced as ``df["col"]`` or ``col("col")`` within a node."""
    columns: set[str] = set()
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Subscript)
            and isinstance(child.slice, ast.Constant)
            and isinstance(child.slice.value, str)
        ):
            columns.add(child.slice.value)
        if (
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == "col"
            and child.args
            and isinstance(child.args[0], ast.Constant)
            and isinstance(child.args[0].value, str)
        ):
            columns.add(child.args[0].value)
    return columns


def _projected_names(select_call: ast.Call) -> set[str]:
    """Output column names of a ``.select(...)`` projection."""
    names: set[str] = set()
    for arg in select_call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            names.add(arg.value)
        if not (isinstance(arg, ast.Call) and arg.args and isinstance(arg.args[0], ast.Constant)):
            continue
        name = arg.args[0].value
        if not isinstance(name, str):
            continue
        if (
            isinstance(arg.func, ast.Attribute)
            and arg.func.attr == "alias"
            or isinstance(arg.func, ast.Name)
            and arg.func.id == "col"
        ):
            names.add(name)
    return names


def _chain_root_name(expr: ast.expr) -> str | None:
    """Base variable of a ``name.method(...).method(...)`` call chain, if any."""
    node: ast.expr = expr
    while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        node = node.func.value
    return node.id if isinstance(node, ast.Name) else None


def _returned_select_columns(tree: ast.Module) -> set[str]:
    """Projection of the select feeding the model's final return statement,
    chased backwards through the intermediate assignments (filters, caches)
    that sit between that select and the return."""
    model_fn = _model_function(tree)
    assigns = [node for node in ast.walk(model_fn) if isinstance(node, ast.Assign)]
    final_return = max(
        (node for node in ast.walk(model_fn) if isinstance(node, ast.Return)),
        key=lambda node: node.lineno,
    )
    assert final_return.value is not None
    expr: ast.expr = final_return.value
    lineno = final_return.lineno
    for _ in range(len(assigns) + 1):
        select_call = next(
            (
                node
                for node in ast.walk(expr)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "select"
            ),
            None,
        )
        if select_call is not None:
            return _projected_names(select_call)
        root = _chain_root_name(expr)
        if root is None:
            break
        prior = [
            assign
            for assign in assigns
            if assign.lineno < lineno
            and any(isinstance(target, ast.Name) and target.id == root for target in assign.targets)
        ]
        if not prior:
            break
        latest = max(prior, key=lambda assign: assign.lineno)
        assert latest.value is not None
        expr, lineno = latest.value, latest.lineno
    raise AssertionError("could not resolve the model's returned select projection")


def test_no_vendor_timestamp_in_watermark_or_gate() -> None:
    """The stored cutoff aggregates the ingest column, and no ordering
    comparison in the model touches a vendor-clock column - either strictness,
    either direction, so a one-character ``>=`` -> ``>`` edit cannot dodge
    the ban."""
    for model_name in MODEL_NAMES:
        tree = _tree(model_name)

        agg_columns = _watermark_agg_columns(tree)
        assert "feed_extracted_at" in agg_columns, model_name
        assert not agg_columns & VENDOR_TIMESTAMP_COLUMNS, model_name

        compared_columns: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare) and any(isinstance(op, ORDERING_OPS) for op in node.ops):
                compared_columns |= _referenced_columns(node)
        assert not compared_columns & VENDOR_TIMESTAMP_COLUMNS, (
            f"{model_name} compares a vendor-clock column: "
            f"{sorted(compared_columns & VENDOR_TIMESTAMP_COLUMNS)}"
        )


def test_missing_gate_column_raises_before_api_work() -> None:
    """A run whose inputs predate the gate column must refuse up front: the
    old fallback crawled the API for a full run and then died on
    on_schema_change="fail" at the merge, spending hours of cluster time and
    API budget on a doomed run."""
    for model_name in MODEL_NAMES:
        model_fn = _model_function(_tree(model_name))

        guard_lines = [
            node.lineno
            for node in ast.walk(model_fn)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Constant)
            and node.test.left.value == "feed_extracted_at"
            and any(isinstance(op, ast.NotIn) for op in node.test.ops)
            and any(
                isinstance(comparator, ast.Attribute) and comparator.attr == "columns"
                for comparator in node.test.comparators
            )
            and any(isinstance(child, ast.Raise) for stmt in node.body for child in ast.walk(stmt))
        ]
        api_lines = [
            node.lineno
            for node in ast.walk(model_fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in API_TOKEN_FACTORIES
        ]

        assert len(guard_lines) >= EXPECTED_GUARDS[model_name], (
            f"{model_name} must raise when a relation lacks feed_extracted_at "
            f"(expected {EXPECTED_GUARDS[model_name]} guard(s), found {len(guard_lines)})"
        )
        assert api_lines, f"{model_name} no longer calls its API token factory; update this test"
        assert max(guard_lines) < min(
            api_lines
        ), f"{model_name} must fail fast on a missing gate column before any API work"


def test_returned_output_carries_feed_extracted_at() -> None:
    """The gate only works if each run's output lands the column the next
    run's cutoff reads. Resolved from the final return statement's select so
    an alias elsewhere in the file (the worklist groupBy, an intermediate
    frame) cannot satisfy the contract."""
    for model_name in MODEL_NAMES:
        returned_columns = _returned_select_columns(_tree(model_name))
        assert "feed_extracted_at" in returned_columns, (
            f"{model_name} must project feed_extracted_at in its returned select, "
            f"got {sorted(returned_columns)}"
        )
