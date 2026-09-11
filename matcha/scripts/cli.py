# scripts/cli.py
"""
CLI entrypoint for entity resolution.

Usage:
    uv run python -m scripts.cli match --entity-type candidacy_stage --input data/input.csv
    uv run python -m scripts.cli match --entity-type elected_official --input catalog.schema.table
    uv run python -m scripts.cli audit summary --entity-type candidacy_stage --results-dir results/candidacy_stage/
"""

import json
from pathlib import Path

import click
import numpy as np
import pandas as pd

from scripts.databricks_io import is_databricks_fqn, read_table, write_table
from scripts.entity_config import ENTITY_TYPES, EntityConfig, get_config
from scripts.person_clustering import cluster_people, summarize
from scripts.pipeline import filter_pairwise, run
from scripts.serialization import json_fallback

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_RESULTS = _PROJECT_DIR / "results"

_ENTITY_TYPE_OPTION = click.option(
    "--entity-type",
    "entity_type",
    default="candidacy_stage",
    type=click.Choice(ENTITY_TYPES, case_sensitive=False),
    help="Entity type to match (default: candidacy_stage).",
)


def _serialize_array_value(v):
    """Convert array/ndarray cell to a JSON string, passing through nulls."""
    if isinstance(v, np.ndarray):
        return json.dumps(v.tolist(), default=json_fallback)
    if isinstance(v, list):
        return json.dumps(v, default=json_fallback)
    return v


def _normalize_to_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Databricks native types to all-string dtypes."""
    for col in df.columns:
        idx = df[col].first_valid_index()
        sample = df[col].loc[idx] if idx is not None else None

        if isinstance(sample, (np.ndarray, list)):
            df[col] = df[col].apply(_serialize_array_value)
        else:
            df[col] = df[col].apply(lambda v: str(v).removesuffix(".0") if pd.notna(v) else v)
    return df


def _load_input(input_value: str) -> pd.DataFrame:
    """Load input DataFrame from CSV path or Databricks FQN."""
    if is_databricks_fqn(input_value):
        print(f"Reading from Databricks: {input_value}")
        df = read_table(input_value)
        return _normalize_to_strings(df)

    path = Path(input_value)
    if not path.exists():
        raise click.BadParameter(f"File not found: {path}")
    print(f"Reading from CSV: {path}")
    return pd.read_csv(path, dtype=str)


def _build_groups(
    pairwise_df: pd.DataFrame,
    config: EntityConfig,
    links_value: str | None,
    nodes_value: str | None,
    output_dir: Path,
    output_groups_table: str | None,
    overwrite: bool,
) -> pd.DataFrame:
    """Close over dbt's deterministic links and admit the scored pairs under the
    clique and cannot-link rules. Inputs default to the config's tables."""
    assert config.graph_inputs is not None
    links_df = _load_input(links_value or config.graph_inputs.links_table)
    nodes_df = _load_input(nodes_value or config.graph_inputs.nodes_table)
    groups_df = cluster_people(pairwise_df, links_df, nodes_df, threshold=config.cluster_threshold)
    print(summarize(groups_df))
    groups_df.to_csv(output_dir / config.groups_output_name, index=False)
    if output_groups_table:
        write_table(groups_df, output_groups_table, overwrite=overwrite)
    return groups_df


_GRAPH_INPUT_OPTIONS = [
    click.option(
        "--links",
        "links_value",
        default=None,
        type=str,
        help="Deterministic link pairs, CSV path or Databricks FQN. Defaults to the config's table.",
    ),
    click.option(
        "--nodes",
        "nodes_value",
        default=None,
        type=str,
        help="Record universe, CSV path or Databricks FQN. Defaults to the config's table.",
    ),
    click.option(
        "--output-groups-table",
        "output_groups_table",
        default=None,
        type=str,
        help="Databricks FQN to upload the canonical groups (catalog.schema.table).",
    ),
]


def _with_graph_input_options(fn):
    for option in reversed(_GRAPH_INPUT_OPTIONS):
        fn = option(fn)
    return fn


def _load_results(results_dir: Path, config: EntityConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load input, pairwise, and clustered DataFrames from a results directory."""
    input_df = pd.read_parquet(results_dir / "input.parquet")
    pairwise_df = pd.read_csv(results_dir / "pairwise_predictions.csv")
    clustered_df = pd.read_csv(results_dir / config.clustered_output_name)
    return input_df, pairwise_df, clustered_df


@click.group()
def cli():
    """Entity resolution CLI."""


@cli.command()
@_ENTITY_TYPE_OPTION
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
    help="Directory for local results. Defaults to results/<entity-type>/.",
)
@click.option(
    "--output-cluster-table",
    "output_cluster_table",
    default=None,
    type=str,
    help="Databricks FQN to upload clustered results (catalog.schema.table).",
)
@click.option(
    "--output-pairwise-table",
    "output_pairwise_table",
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
@_with_graph_input_options
def match(
    entity_type: str,
    input_value: str,
    output_dir: Path | None,
    output_cluster_table: str | None,
    output_pairwise_table: str | None,
    overwrite: bool,
    run_audit: bool,
    links_value: str | None,
    nodes_value: str | None,
    output_groups_table: str | None,
) -> None:
    """Run Splink entity resolution on prematch data."""
    config = get_config(entity_type)
    if config.graph_inputs is None and (links_value or nodes_value or output_groups_table):
        raise click.BadParameter(
            f"--links, --nodes and --output-groups-table apply only to entities with graph inputs, not {entity_type}"
        )

    if output_dir is None:
        output_dir = _DEFAULT_RESULTS / config.entity_type

    input_df = _load_input(input_value)
    pairwise_df, clustered_df = run(input_df=input_df, output_dir=output_dir, config=config)

    if pairwise_df.empty and clustered_df.empty:
        raise click.ClickException(
            "Entity resolution produced 0 matches. This likely indicates a "
            "problem with the input data or matching configuration."
        )

    # Persist input so standalone audit commands can read it later
    input_df.to_parquet(output_dir / "input.parquet", index=False)

    # Upload to Databricks if requested
    if output_cluster_table or output_pairwise_table:
        if output_cluster_table:
            write_table(clustered_df, output_cluster_table, overwrite=overwrite)
        if output_pairwise_table:
            write_table(pairwise_df, output_pairwise_table, overwrite=overwrite)

    if config.graph_inputs is not None:
        _build_groups(
            pairwise_df, config, links_value, nodes_value, output_dir, output_groups_table, overwrite
        )

    # Run audits
    if run_audit and len(clustered_df) > 0:
        print("\n── Running audits ──")
        from scripts.audit_false_negatives import run_false_negatives
        from scripts.audit_low_confidence import run_low_confidence
        from scripts.audit_summary import run_summary

        for label, fn, args in [
            ("summary", run_summary, (input_df, pairwise_df, clustered_df, output_dir, config)),
            ("low-confidence", run_low_confidence, (pairwise_df, output_dir, config)),
            (
                "false-negatives",
                run_false_negatives,
                (input_df, pairwise_df, clustered_df, output_dir, config),
            ),
        ]:
            try:
                fn(*args)
            except Exception as e:
                print(f"Audit {label} failed: {e}")


@cli.command()
@_ENTITY_TYPE_OPTION
@click.option(
    "--pairwise",
    "pairwise_value",
    required=True,
    type=str,
    help="Scored pairs from a published run, CSV path or Databricks FQN.",
)
@click.option(
    "--output-dir",
    "output_dir",
    default=None,
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory for the groups CSV. Defaults to results/<entity-type>_recluster/.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite an existing Databricks output table.",
)
@_with_graph_input_options
def recluster(
    entity_type: str,
    pairwise_value: str,
    output_dir: Path | None,
    overwrite: bool,
    links_value: str | None,
    nodes_value: str | None,
    output_groups_table: str | None,
) -> None:
    """Rebuild canonical groups from an already-scored pairwise vintage.

    Applies the config's current post-prediction filters, then closes over the
    deterministic links and admits pairs under the clique and cannot-link rules.
    No Splink scoring, so a rule change or fresh dbt links can be re-published
    without re-training.
    """
    config = get_config(entity_type)
    if config.graph_inputs is None:
        raise click.BadParameter(f"{entity_type} has no graph inputs to recluster over")
    if output_dir is None:
        output_dir = _DEFAULT_RESULTS / f"{config.entity_type}_recluster"
    output_dir.mkdir(parents=True, exist_ok=True)

    pairwise_df = filter_pairwise(_load_input(pairwise_value), config)
    _build_groups(pairwise_df, config, links_value, nodes_value, output_dir, output_groups_table, overwrite)


# ── Audit subcommands ──


def _resolve_results_dir(results_dir: Path | None, config: EntityConfig) -> Path:
    """Default results_dir to results/<entity_type>/ when not provided."""
    if results_dir is not None:
        return results_dir
    return _DEFAULT_RESULTS / config.entity_type


@cli.group()
def audit():
    """Audit match results for quality."""


@audit.command("summary")
@_ENTITY_TYPE_OPTION
@click.option(
    "--results-dir",
    default=None,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing match results.",
)
def audit_summary(entity_type: str, results_dir: Path | None) -> None:
    """Print match summary statistics."""
    from scripts.audit_summary import run_summary

    config = get_config(entity_type)
    results_dir = _resolve_results_dir(results_dir, config)
    input_df, pairwise_df, clustered_df = _load_results(results_dir, config)
    run_summary(input_df, pairwise_df, clustered_df, results_dir, config)


@audit.command("low-confidence")
@_ENTITY_TYPE_OPTION
@click.option(
    "--results-dir",
    default=None,
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
def audit_low_confidence(entity_type: str, results_dir: Path | None, sample_n: int) -> None:
    """Find the most ambiguous matches for manual review."""
    from scripts.audit_low_confidence import run_low_confidence

    config = get_config(entity_type)
    results_dir = _resolve_results_dir(results_dir, config)
    _, pairwise_df, _ = _load_results(results_dir, config)
    run_low_confidence(pairwise_df, results_dir, config, sample_n)


@audit.command("false-negatives")
@_ENTITY_TYPE_OPTION
@click.option(
    "--results-dir",
    default=None,
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
def audit_false_negatives(entity_type: str, results_dir: Path | None, sample_n: int) -> None:
    """Find plausible matches that the model missed."""
    from scripts.audit_false_negatives import run_false_negatives

    config = get_config(entity_type)
    results_dir = _resolve_results_dir(results_dir, config)
    input_df, pairwise_df, clustered_df = _load_results(results_dir, config)
    run_false_negatives(input_df, pairwise_df, clustered_df, results_dir, config, sample_n)


if __name__ == "__main__":
    cli()
