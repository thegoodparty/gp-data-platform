"""Source-contract tests for the BallotReady enrichment incremental gates.

The candidacy and person intermediates are incremental Python models that only
execute on a Databricks cluster, so the gate discipline is pinned by parsing
the source. The property that matters: the incremental cutoff is the feed's
Airbyte ingest timestamp, never a vendor updated_at. Vendor backfills deliver
new files whose rows carry old vendor timestamps, and a vendor-timestamp
watermark skips those rows on every run; our ingest clock is monotonic, so an
ingest-time gate always admits newly delivered files.
"""

import ast
from pathlib import Path

import pytest

MODELS_DIR = Path(__file__).parent.parent / "project" / "models" / "intermediate" / "ballotready_api"

# model name -> the staged-input column its incremental filter must gate on
GATE_COLUMNS = {
    "int__ballotready_candidacy": "_airbyte_extracted_at",
    "int__ballotready_person": "feed_extracted_at",
}

# Vendor-clock columns that must never be the incremental watermark again.
VENDOR_TIMESTAMP_COLUMNS = {"updated_at", "candidacy_updated_at"}


def _tree(model_name: str) -> ast.Module:
    return ast.parse((MODELS_DIR / f"{model_name}.py").read_text())


def _agg_dict_max_columns(tree: ast.Module) -> set[str]:
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


def _gte_filter_columns(tree: ast.Module) -> set[str]:
    """Columns compared with ``df["col"] >= ...`` (the incremental filter shape)."""
    columns: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Compare) and any(isinstance(op, ast.GtE) for op in node.ops)):
            continue
        for side in [node.left, *node.comparators]:
            if (
                isinstance(side, ast.Subscript)
                and isinstance(side.slice, ast.Constant)
                and isinstance(side.slice.value, str)
            ):
                columns.add(side.slice.value)
    return columns


@pytest.mark.parametrize("model_name", sorted(GATE_COLUMNS))
def test_cutoff_is_read_from_feed_extracted_at(model_name: str):
    """The existing-table watermark aggregates the ingest column, not a vendor one."""
    agg_columns = _agg_dict_max_columns(_tree(model_name))

    assert "feed_extracted_at" in agg_columns
    assert not agg_columns & VENDOR_TIMESTAMP_COLUMNS


@pytest.mark.parametrize(("model_name", "gate_column"), sorted(GATE_COLUMNS.items()))
def test_incremental_filter_gates_on_ingest_time(model_name: str, gate_column: str):
    """Every ``>=`` subscript filter compares the ingest column; vendor timestamps are out."""
    filter_columns = _gte_filter_columns(_tree(model_name))

    assert gate_column in filter_columns
    assert not filter_columns & VENDOR_TIMESTAMP_COLUMNS


@pytest.mark.parametrize("model_name", sorted(GATE_COLUMNS))
def test_cutoff_read_is_guarded_for_a_missing_column(model_name: str):
    """The first post-deploy incremental runs against a table that does not have
    the new column yet; the cutoff read must check for it instead of throwing."""
    guarded = any(
        isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Constant)
        and node.left.value == "feed_extracted_at"
        and any(isinstance(op, ast.In) for op in node.ops)
        and any(
            isinstance(comparator, ast.Attribute) and comparator.attr == "columns"
            for comparator in node.comparators
        )
        for node in ast.walk(_tree(model_name))
    )

    assert guarded, f"{model_name} must check feed_extracted_at exists before aggregating it"


@pytest.mark.parametrize("model_name", sorted(GATE_COLUMNS))
def test_output_carries_feed_extracted_at(model_name: str):
    """The gate only works if each run's output lands the column the next run reads."""
    aliased = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "alias"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "feed_extracted_at"
        for node in ast.walk(_tree(model_name))
    )

    assert aliased, f"{model_name} must alias feed_extracted_at into its output"
