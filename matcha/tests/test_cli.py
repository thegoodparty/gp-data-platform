# tests/test_cli.py
"""Unit tests for the CLI entrypoint."""

import datetime
import decimal
import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from click.testing import CliRunner

from scripts.cli import (
    _json_fallback,
    _load_input,
    _load_results,
    _normalize_to_strings,
    _serialize_array_value,
    cli,
)
from scripts.configs.candidacy import CANDIDACY_CONFIG

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


def test_serialize_array_value_nested_arrays():
    """Nested arrays serialize; ndarray.tolist() only unwraps the outer level.

    Arrow returns a list<list<string>> column as an object ndarray whose cells
    are themselves ndarrays, which json cannot serialize directly.
    """
    nested = np.empty(1, dtype=object)
    nested[0] = np.array(["x", "y"], dtype=object)

    assert json.loads(_serialize_array_value(nested)) == [["x", "y"]]


def test_serialize_array_value_numpy_scalars():
    """numpy scalars inside a list serialize as plain JSON numbers."""
    assert json.loads(_serialize_array_value([np.int64(1), np.float64(2.5)])) == [1, 2.5]


# --- _normalize_to_strings: the production input path -------------------------
# read_table() hands this function the result of fetchall_arrow().to_pandas(),
# so these build frames the same way rather than from Python literals. That is
# what makes them sensitive to the dtype inference pandas applies on the way in.


def _arrow_frame(arrow_type, values):
    """One-column frame shaped exactly as read_table would produce it."""
    return pa.table({"c": pa.array(values, arrow_type)}).to_pandas()


@pytest.mark.parametrize(
    ("arrow_type", "values", "expected"),
    [
        (pa.string(), ["bob"], "bob"),
        (pa.int64(), [1234], "1234"),
        (pa.int32(), [7], "7"),
        (pa.float64(), [5.0], "5"),  # trailing ".0" stripped for BIGINT round-trip
        (pa.float64(), [0.9], "0.9"),
        (pa.bool_(), [True], "True"),
        (pa.decimal128(10, 2), [decimal.Decimal("1234.56")], "1234.56"),
        (pa.timestamp("us"), [datetime.datetime(2026, 11, 3, 10, 30)], "2026-11-03 10:30:00"),
        (pa.date32(), [datetime.date(2026, 11, 3)], "2026-11-03"),
    ],
)
def test_normalize_to_strings_stringifies_warehouse_types(arrow_type, values, expected):
    out = _normalize_to_strings(_arrow_frame(arrow_type, values))
    assert out["c"].tolist() == [expected]


@pytest.mark.parametrize(
    ("arrow_type", "values"),
    [
        (pa.string(), ["bob", None]),
        (pa.int64(), [1, None]),
        (pa.float64(), [1.5, None]),
        (pa.bool_(), [True, None]),
        (pa.decimal128(10, 2), [decimal.Decimal("5.00"), None]),
        (pa.date32(), [datetime.date(2026, 11, 3), None]),
        (pa.list_(pa.string()), [["a"], None]),
    ],
)
def test_normalize_to_strings_keeps_nulls_null(arrow_type, values):
    """Nulls stay null and never become the text "None"/"nan"/"<NA>"/"NaT".

    Asserts null-ness rather than a specific sentinel: pandas 2 carries None in
    object columns where pandas 3 carries NaN, and both are SQL NULL downstream.
    """
    out = _normalize_to_strings(_arrow_frame(arrow_type, values))

    assert pd.isna(out["c"].iloc[1])
    assert not any(isinstance(v, str) and v in {"None", "nan", "<NA>", "NaT"} for v in out["c"].tolist())


def test_normalize_to_strings_serializes_list_columns():
    """list<string> becomes a JSON string so load_and_prepare can json.loads it."""
    out = _normalize_to_strings(_arrow_frame(pa.list_(pa.string()), [["bob", "robert"]]))
    assert json.loads(out["c"].iloc[0]) == ["bob", "robert"]


def test_normalize_to_strings_detects_array_column_after_leading_null():
    """The array branch is chosen from the first NON-null cell.

    A leading null must not push an array column down the str() branch, which
    would stringify the array as its repr instead of JSON.
    """
    out = _normalize_to_strings(_arrow_frame(pa.list_(pa.string()), [None, ["bob"]]))
    assert json.loads(out["c"].iloc[1]) == ["bob"]


def test_normalize_to_strings_handles_all_null_column():
    """An all-null column has no sample to inspect and must not raise."""
    out = _normalize_to_strings(_arrow_frame(pa.string(), [None, None]))
    assert out["c"].isna().all()


def test_json_fallback_rejects_unknown_types():
    """The hook unwraps numpy only; anything else is still a real error."""
    with pytest.raises(TypeError, match="not JSON serializable"):
        _json_fallback(object())


# --- _load_input / _load_results: the pandas IO boundary -----------------------


@patch("scripts.cli._normalize_to_strings", side_effect=lambda df: df)
@patch("scripts.cli.read_table")
def test_load_input_reads_databricks_fqn(mock_read, mock_norm):
    """A three-part FQN routes to read_table and then through normalization."""
    mock_read.return_value = pd.DataFrame({"unique_id": ["1"]})

    out = _load_input("cat.sch.tbl")

    mock_read.assert_called_once_with("cat.sch.tbl")
    mock_norm.assert_called_once()
    assert out["unique_id"].tolist() == ["1"]


def test_load_input_reads_csv_as_all_strings(tmp_path):
    """CSV input is read with dtype=str so numeric-looking ids keep their form."""
    csv = tmp_path / "in.csv"
    csv.write_text("unique_id,n_votes\n007,1234\n")

    out = _load_input(str(csv))

    assert out["unique_id"].tolist() == ["007"]
    assert out["n_votes"].tolist() == ["1234"]


def test_load_input_rejects_missing_csv(tmp_path):
    import click

    with pytest.raises(click.BadParameter):
        _load_input(str(tmp_path / "nope.csv"))


def _write_results_dir(results_dir: Path) -> None:
    """Lay down the three files a completed match run leaves behind."""
    results_dir.mkdir(parents=True, exist_ok=True)
    # Carries the grouping columns the false-negatives audit needs, so this
    # matches the shape a real match run leaves behind.
    pd.DataFrame(
        {
            "unique_id": ["b1", "t1"],
            "source_name": ["ballotready", "techspeed"],
            "state": ["WI", "WI"],
            "election_date": ["2026-11-03", "2026-11-03"],
            "first_name": ["jane", "jane"],
            "last_name": ["doe", "doe"],
        }
    ).to_parquet(results_dir / "input.parquet", index=False)
    pd.DataFrame(
        {
            "unique_id_l": ["b1"],
            "unique_id_r": ["t1"],
            "source_name_l": ["ballotready"],
            "source_name_r": ["techspeed"],
            "match_probability": [0.97],
            "match_weight": [3.2],
            "gamma_last_name": [2],
        }
    ).to_csv(results_dir / "pairwise_predictions.csv", index=False)
    pd.DataFrame(
        {
            "unique_id": ["b1", "t1"],
            "cluster_id": [1, 1],
            "source_name": ["ballotready", "techspeed"],
        }
    ).to_csv(results_dir / CANDIDACY_CONFIG.clustered_output_name, index=False)


def test_load_results_reads_all_three_artifacts(tmp_path):
    _write_results_dir(tmp_path)

    input_df, pairwise_df, clustered_df = _load_results(tmp_path, CANDIDACY_CONFIG)

    assert input_df["unique_id"].tolist() == ["b1", "t1"]
    assert pairwise_df["match_probability"].tolist() == [0.97]
    assert clustered_df["cluster_id"].tolist() == [1, 1]


# --- audit subcommands --------------------------------------------------------


@pytest.mark.parametrize(
    ("subcommand", "expected_file"),
    [
        ("summary", "audit_summary.csv"),
        ("low-confidence", "audit_low_confidence.csv"),
    ],
)
def test_audit_subcommands_write_their_csv(tmp_path, subcommand, expected_file):
    """Each audit subcommand loads the results dir and writes its own CSV."""
    _write_results_dir(tmp_path)

    result = CliRunner().invoke(
        cli, ["audit", subcommand, "--entity-type", "candidacy_stage", "--results-dir", str(tmp_path)]
    )

    assert result.exit_code == 0, f"{result.output}\n{result.exception}"
    assert (tmp_path / expected_file).exists()


def test_audit_false_negatives_runs_on_a_results_dir(tmp_path):
    """false-negatives completes even when it finds nothing to report."""
    _write_results_dir(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "audit",
            "false-negatives",
            "--entity-type",
            "candidacy_stage",
            "--results-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, f"{result.output}\n{result.exception}"


@patch("scripts.cli.write_table")
@patch("scripts.cli.run", side_effect=_fake_run)
def test_match_writes_output_tables_when_requested(mock_run, mock_write, tmp_path):
    """--output-*-table routes the result frames to write_table."""
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
            "--output-cluster-table",
            "cat.sch.clustered",
            "--output-pairwise-table",
            "cat.sch.pairwise",
        ],
    )

    assert result.exit_code == 0, f"{result.output}\n{result.exception}"
    written = {c.args[1] for c in mock_write.call_args_list}
    assert written == {"cat.sch.clustered", "cat.sch.pairwise"}
