"""Source-contract test for the BallotReady enrichment incremental gates.

The candidacy and person intermediates are incremental Python models that only
execute on a Databricks cluster, so the gate discipline is pinned by parsing
the source: each model's stored cutoff must aggregate feed_extracted_at, the
feed's Airbyte ingest timestamp. Vendor backfills deliver files whose rows
carry old vendor timestamps, so a vendor-timestamp cutoff skips them forever;
the ingest clock is monotonic on our side.
"""

import ast
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent / "project" / "models" / "intermediate" / "ballotready_api"

MODEL_NAMES = ("int__ballotready_candidacy", "int__ballotready_person")


def _watermark_agg_columns(model_name: str) -> set[str]:
    """Columns read via the dict-style ``.agg({"col": "max"})`` cutoff pattern."""
    tree = ast.parse((MODELS_DIR / f"{model_name}.py").read_text())
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


def test_incremental_cutoff_aggregates_feed_ingest_time() -> None:
    """Each model's incremental cutoff must read the feed's ingest timestamp;
    a cutoff aggregating only vendor updated_at values would strand backfilled
    files whose rows carry old vendor timestamps."""
    for model_name in MODEL_NAMES:
        assert "feed_extracted_at" in _watermark_agg_columns(
            model_name
        ), f"{model_name} must aggregate feed_extracted_at for its incremental cutoff"
