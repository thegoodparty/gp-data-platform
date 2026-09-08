"""retl: `retl --source <flow> --destination <hubspot_contacts|csv>`.

Config is entirely environment-driven. The destination choice is a plain if/else
-- two destinations are not architecture. Every exit prints one summary line, plus
row-level error detail to stderr when there were any, so a DAG task wrapping this
subprocess can carry counts and error codes into its own failure alert instead of
a bare exit code.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from . import csv_destination, databricks_io, hubspot_destination
from .config import load_flow_config, load_log_table
from .destinations import Destination
from .run import error_report_lines, execute_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diff a Databricks desired-state model against a destination"
    )
    parser.add_argument("--source", required=True, help="Flow name; selects RETL_FLOW_<NAME>_* config")
    parser.add_argument(
        "--destination",
        required=True,
        choices=["hubspot_contacts", "csv"],
        help="Where to deliver the diff",
    )
    return parser


def _build_destination(name: str, env: dict[str, str]) -> Destination:
    if name == "hubspot_contacts":
        return hubspot_destination.HubSpotDestination(hubspot_destination.config_from_env(env))
    return csv_destination.CsvDestination(csv_destination.config_from_env(env))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = dict(os.environ)

    try:
        flow = load_flow_config(args.source, env)
        log_table = load_log_table(env)
        destination = _build_destination(args.destination, env)
        connection = databricks_io.connect(databricks_io.config_from_env(env))
        try:
            summary = execute_run(
                connection=connection, flow=flow, log_table=log_table, destination=destination
            )
        finally:
            connection.close()
    except Exception as exc:
        # Broad on purpose: this is the process boundary. The DAG's own wrapper (T4)
        # captures this text to carry counts into its failure alert; a narrower catch
        # here would just mean some failures print nothing useful before exiting.
        print(f"retl FAILED: {exc}", file=sys.stderr)
        return 1

    print(summary.as_line())
    for line in error_report_lines(summary.errors):
        print(line, file=sys.stderr)
    return 0 if summary.error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
