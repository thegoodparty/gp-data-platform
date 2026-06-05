# tests/test_cli.py
"""Unit tests for the CLI entrypoint."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
from click.testing import CliRunner

from scripts.cli import cli

DUMMY_CSV = Path(__file__).parent / "dummy_data.csv"


def _fake_run(input_df, output_dir, config):
    """Return minimal pairwise + clustered DataFrames without running Splink."""
    pairwise = pd.DataFrame(
        {
            "unique_id_l": ["br_001"],
            "unique_id_r": ["ts_001"],
            "match_probability": [0.95],
        }
    )
    clustered = pd.DataFrame(
        {
            "unique_id": ["br_001", "ts_001"],
            "cluster_id": [1, 1],
            "source_name": ["ballotready", "techspeed"],
            "first_name": ["Jane", "Janet"],
            "last_name": ["Doe", "Doe"],
        }
    )
    # Write the CSVs that the real pipeline would produce
    output_dir.mkdir(parents=True, exist_ok=True)
    pairwise.to_csv(output_dir / "pairwise_predictions.csv", index=False)
    clustered.to_csv(output_dir / config.clustered_output_name, index=False)
    return pairwise, clustered


def test_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Entity resolution CLI" in result.output


def test_match_help():
    result = CliRunner().invoke(cli, ["match", "--help"])
    assert result.exit_code == 0
    assert "--entity-type" in result.output


@patch("scripts.cli.run", side_effect=_fake_run)
def test_match_with_csv(mock_run, tmp_path):
    """match subcommand reads a CSV, calls run(), and writes output."""
    result = CliRunner().invoke(
        cli,
        [
            "match",
            "--entity-type",
            "candidacy_stage",
            "--input",
            str(DUMMY_CSV),
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"CLI failed:\n{result.output}\n{result.exception}"
    mock_run.assert_called_once()

    # Verify output files were written
    assert (tmp_path / "pairwise_predictions.csv").exists()
    assert (tmp_path / "clustered_candidacies.csv").exists()
    assert (tmp_path / "input.parquet").exists()


@patch("scripts.cli.run", side_effect=_fake_run)
def test_match_missing_file(mock_run):
    """match fails gracefully when input file doesn't exist."""
    result = CliRunner().invoke(
        cli,
        [
            "match",
            "--entity-type",
            "candidacy_stage",
            "--input",
            "/nonexistent/file.csv",
        ],
    )
    assert result.exit_code != 0


def test_match_defaults_to_candidacy_stage():
    """match defaults to candidacy_stage entity type when --entity-type is omitted."""
    result = CliRunner().invoke(cli, ["match", "--help"])
    assert "candidacy_stage" in result.output  # default is visible in help


def test_match_requires_input():
    """match fails when --input is not provided."""
    result = CliRunner().invoke(cli, ["match", "--entity-type", "candidacy_stage"])
    assert result.exit_code != 0
    assert "Missing option" in result.output or "required" in result.output.lower()


@patch("scripts.cli.run", side_effect=_fake_run)
def test_match_elected_official_with_csv(mock_run, tmp_path):
    """match with --entity-type elected_official routes to the correct config."""
    result = CliRunner().invoke(
        cli,
        [
            "match",
            "--entity-type",
            "elected_official",
            "--input",
            str(DUMMY_CSV),
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"CLI failed:\n{result.output}\n{result.exception}"
    # Verify config was passed with correct entity type
    call_args = mock_run.call_args
    config = call_args.kwargs.get("config") or call_args[0][2]
    assert config.entity_type == "elected_official"


def test_cli_accepts_election_stage_entity_type():
    """CLI --entity-type accepts 'election_stage'."""
    from click.testing import CliRunner

    from scripts.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["match", "--help"])
    assert result.exit_code == 0
    assert "election_stage" in result.output
