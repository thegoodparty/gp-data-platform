"""
CLI entrypoint for entity resolution.

Usage:
    cd entity_resolution
    uv run python scripts/cli.py match --input data/input.csv
    uv run python scripts/cli.py match --input catalog.schema.table --output-table catalog.schema.output
    uv run python scripts/cli.py audit summary --results-dir results/
    uv run python scripts/cli.py audit low-confidence --results-dir results/ --sample 20
    uv run python scripts/cli.py audit false-negatives --results-dir results/
"""

import json
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd

# Allow importing sibling modules when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline import run  # noqa: E402

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_RESULTS = _PROJECT_DIR / "results"


def _load_input(input_value: str) -> pd.DataFrame:
    """Load input DataFrame from CSV path or Databricks FQN."""
    from databricks_io import is_databricks_fqn

    if is_databricks_fqn(input_value):
        from databricks_io import read_table

        print(f"Reading from Databricks: {input_value}")
        df = read_table(input_value)
        # Normalize to all-string dtypes to match pd.read_csv(dtype=str) behavior.
        # Databricks returns native types which can cause Splink EM training to
        # fail (e.g. "m values not fully trained") because it handles typed
        # columns differently during parameter estimation.
        for col in df.columns:
            sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
            if isinstance(sample, np.ndarray):
                # Array columns (e.g. first_name_aliases) → JSON strings
                df[col] = df[col].apply(
                    lambda v: json.dumps(v.tolist()) if isinstance(v, np.ndarray) else v
                )
            else:
                df[col] = df[col].where(df[col].isna(), df[col].astype(str))
        return df
    else:
        path = Path(input_value)
        if not path.exists():
            raise click.BadParameter(f"File not found: {path}")
        print(f"Reading from CSV: {path}")
        return pd.read_csv(path, dtype=str)


def _load_results(results_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load input, pairwise, and clustered DataFrames from a results directory."""
    input_df = pd.read_parquet(results_dir / "input.parquet")
    pairwise_df = pd.read_csv(results_dir / "pairwise_predictions.csv")
    clustered_df = pd.read_csv(results_dir / "clustered_candidacies.csv")
    return input_df, pairwise_df, clustered_df


@click.group()
def cli():
    """Entity resolution CLI."""


@cli.command()
@click.option(
    "--input",
    "input_value",
    required=True,
    type=str,
    help="Path to prematch CSV file or Databricks FQN (catalog.schema.table).",
)
@click.option(
    "--output-dir",
    "output_dir",
    default=None,
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory for local results. Defaults to results/ in the project root.",
)
@click.option(
    "--output-table",
    "output_table",
    default=None,
    type=str,
    help="Databricks FQN to upload clustered results (catalog.schema.table).",
)
@click.option(
    "--pairwise-table",
    "pairwise_table",
    default=None,
    type=str,
    help="Databricks FQN to upload pairwise predictions (catalog.schema.table).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing Databricks output tables.",
)
@click.option(
    "--run-audit/--no-audit",
    "run_audit",
    default=True,
    help="Run audit reports after matching (default: enabled).",
)
def match(
    input_value: str,
    output_dir: Path | None,
    output_table: str | None,
    pairwise_table: str | None,
    overwrite: bool,
    run_audit: bool,
) -> None:
    """Run Splink entity resolution on prematch data."""
    if output_dir is None:
        output_dir = _DEFAULT_RESULTS

    input_df = _load_input(input_value)
    pairwise_df, clustered_df = run(input_df=input_df, output_dir=output_dir)

    # Persist input so standalone audit commands can read it
    input_df.to_parquet(output_dir / "input.parquet", index=False)

    # Upload to Databricks if requested
    if output_table or pairwise_table:
        from databricks_io import write_table

        if output_table:
            write_table(clustered_df, output_table, overwrite=overwrite)
        if pairwise_table:
            write_table(pairwise_df, pairwise_table, overwrite=overwrite)

    # Run audits
    if run_audit and len(clustered_df) > 0:
        print("\n── Running audits ──")
        from audit_false_negatives import run_false_negatives
        from audit_low_confidence import run_low_confidence
        from audit_summary import run_summary

        for label, fn, args in [
            ("summary", run_summary, (input_df, pairwise_df, clustered_df, output_dir)),
            ("low-confidence", run_low_confidence, (pairwise_df, output_dir)),
            (
                "false-negatives",
                run_false_negatives,
                (input_df, pairwise_df, clustered_df, output_dir),
            ),
        ]:
            try:
                fn(*args)
            except Exception as e:
                print(f"Audit {label} failed: {e}")


# ── Audit subcommands ──


@cli.group()
def audit():
    """Audit match results for quality."""


@audit.command("summary")
@click.option(
    "--results-dir",
    default=str(_DEFAULT_RESULTS),
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing match results.",
)
def audit_summary(results_dir: Path) -> None:
    """Print match summary statistics."""
    from audit_summary import run_summary

    input_df, pairwise_df, clustered_df = _load_results(results_dir)
    run_summary(input_df, pairwise_df, clustered_df, results_dir)


@audit.command("low-confidence")
@click.option(
    "--results-dir",
    default=str(_DEFAULT_RESULTS),
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing match results.",
)
@click.option(
    "--sample",
    "sample_n",
    default=20,
    type=int,
    help="Number of most-ambiguous pairs to return.",
)
def audit_low_confidence(results_dir: Path, sample_n: int) -> None:
    """Find the most ambiguous matches for manual review."""
    from audit_low_confidence import run_low_confidence

    _, pairwise_df, _ = _load_results(results_dir)
    run_low_confidence(pairwise_df, results_dir, sample_n)


@audit.command("false-negatives")
@click.option(
    "--results-dir",
    default=str(_DEFAULT_RESULTS),
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing match results.",
)
@click.option(
    "--sample",
    "sample_n",
    default=20,
    type=int,
    help="Number of suspicious pairs to find.",
)
def audit_false_negatives(results_dir: Path, sample_n: int) -> None:
    """Find plausible matches that the model missed."""
    from audit_false_negatives import run_false_negatives

    input_df, pairwise_df, clustered_df = _load_results(results_dir)
    run_false_negatives(input_df, pairwise_df, clustered_df, results_dir, sample_n)


if __name__ == "__main__":
    cli()
