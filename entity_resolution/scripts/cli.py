"""
CLI entrypoint for entity resolution.

Usage:
    cd entity_resolution
    uv run python scripts/cli.py match --input data/input.csv
    uv run python scripts/cli.py match --input data/input.csv --output-dir results/
"""

import sys
from pathlib import Path

import click

# Allow importing sibling modules when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent))

from initial_match import run  # noqa: E402


@click.group()
def cli():
    """Run BallotReady x TechSpeed entity resolution."""


@cli.command()
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the prematch CSV file.",
)
@click.option(
    "--output-dir",
    "output_dir",
    default=None,
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory for results. Defaults to results/ in the project root.",
)
def match(input_path: Path, output_dir: Path | None) -> None:
    """Run Splink entity resolution on a prematch CSV."""
    if output_dir is None:
        output_dir = Path(__file__).resolve().parent.parent / "results"

    run(input_path=input_path, output_dir=output_dir)


# Future: add a `from-databricks` command that reads a FQN relation
# @cli.command()
# @click.option("--relation", required=True, help="Fully-qualified Databricks table name.")
# @click.option("--output-dir", ...)
# def from_databricks(relation: str, output_dir: Path | None) -> None:
#     """Run entity resolution reading from a Databricks relation."""
#     ...


if __name__ == "__main__":
    cli()
