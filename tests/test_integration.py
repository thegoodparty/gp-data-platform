"""Integration tests: CLI round-trip through Databricks.

Run with:  uv run pytest -m integration
"""

import pytest
from click.testing import CliRunner


@pytest.mark.integration
def test_match_databricks_round_trip(databricks_tables):
    """Full pipeline: read from Databricks, run Splink, write results back."""
    from scripts.cli import cli
    from scripts.databricks_io import read_table

    ctx = databricks_tables
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "match",
            "--entity-type",
            "candidacy_stage",
            "--input",
            ctx["input_fqn"],
            "--output-cluster-table",
            ctx["output_cluster_fqn"],
            "--output-pairwise-table",
            ctx["output_pairwise_fqn"],
            "--output-dir",
            str(ctx["output_dir"]),
            "--overwrite",
        ],
    )

    assert result.exit_code == 0, f"CLI failed:\n{result.output}\n{result.exception}"

    # ── Read back from Databricks ──
    clustered_df = read_table(ctx["output_cluster_fqn"])
    pairwise_df = read_table(ctx["output_pairwise_fqn"])

    # ── Clustered table assertions ──
    assert len(clustered_df) > 0, "Clustered table is empty"
    for col in ["unique_id", "cluster_id", "source_name", "first_name", "last_name"]:
        assert col in clustered_df.columns, f"Missing column: {col}"
    assert clustered_df["cluster_id"].notna().all(), "cluster_id has nulls"

    # ── Pairwise table assertions ──
    assert len(pairwise_df) > 0, "Pairwise table is empty"
    for col in ["unique_id_l", "unique_id_r", "match_probability"]:
        assert col in pairwise_df.columns, f"Missing column: {col}"

    # ── Local output files ──
    assert (ctx["output_dir"] / "pairwise_predictions.csv").exists()
    assert (ctx["output_dir"] / "clustered_candidacies.csv").exists()
    assert (ctx["output_dir"] / "input.parquet").exists()

    # ── At least 1 cross-source cluster (proves matching worked) ──
    multi_source = (
        clustered_df.groupby("cluster_id")["source_name"].nunique() > 1
    ).sum()
    assert (
        multi_source >= 1
    ), f"Expected at least 1 cross-source cluster, got {multi_source}"


@pytest.mark.integration
def test_match_elected_officials_databricks_round_trip(databricks_tables_eo):
    """Elected officials pipeline: read from Databricks, run Splink, write results back."""
    from scripts.cli import cli
    from scripts.databricks_io import read_table

    ctx = databricks_tables_eo
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "match",
            "--entity-type",
            "elected_official",
            "--input",
            ctx["input_fqn"],
            "--output-cluster-table",
            ctx["output_cluster_fqn"],
            "--output-pairwise-table",
            ctx["output_pairwise_fqn"],
            "--output-dir",
            str(ctx["output_dir"]),
            "--overwrite",
        ],
    )

    assert result.exit_code == 0, f"CLI failed:\n{result.output}\n{result.exception}"

    clustered_df = read_table(ctx["output_cluster_fqn"])
    assert len(clustered_df) > 0, "Clustered table is empty"
    assert "cluster_id" in clustered_df.columns


def test_election_stage_end_to_end(tmp_path):
    """End-to-end run on the election_stage fixture.

    Expects:
    - rows 1001 + 2001 in same cluster (BR + DDHQ regular general)
    - rows 1002 + 2002 in same cluster (BR + DDHQ special general)
    - row 1004 NOT in cluster with 1001 (different year)
    - row 2003 NOT in any of the other clusters (different state)
    - row 1001 (regular) NOT in cluster with 1002 (special)

    Note: the cluster_threshold is lowered to 0.8 for this test because the
    20-row fixture is too small for EM to converge on m probabilities that
    push true matches above 0.95. The fixture exercises clustering
    *relationships*, not the production threshold (which is validated on
    real-scale data via the manual matcher run).
    """
    from dataclasses import replace
    from pathlib import Path

    import pandas as pd

    from scripts.entity_config import get_config
    from scripts.pipeline import run

    fixture = Path(__file__).parent / "fixtures" / "election_stage_input.csv"
    config = replace(get_config("election_stage"), cluster_threshold=0.8)

    input_df = pd.read_csv(fixture, dtype=str)
    output_dir = tmp_path / "election_stage"
    output_dir.mkdir()
    run(input_df=input_df, output_dir=output_dir, config=config)

    clusters = pd.read_csv(output_dir / "clustered_election_stages.csv")
    cl_by_id = dict(zip(clusters["unique_id"], clusters["cluster_id"]))

    # Regular general: BR + DDHQ should cluster
    assert cl_by_id["ballotready|1001"] == cl_by_id["ddhq|2001"]
    # Special general: BR + DDHQ should cluster
    assert cl_by_id["ballotready|1002"] == cl_by_id["ddhq|2002"]
    # Different year must not collide
    assert cl_by_id["ballotready|1001"] != cl_by_id["ballotready|1004"]
    # Different state must not collide
    assert cl_by_id["ddhq|2003"] != cl_by_id["ballotready|1001"]
    # Special and regular for the same office+year must not collide
    assert cl_by_id["ballotready|1001"] != cl_by_id["ballotready|1002"]
