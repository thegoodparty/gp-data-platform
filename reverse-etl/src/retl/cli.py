"""retl: `retl --source <flow> --destination <hubspot_contacts|csv>`.

Config is entirely environment-driven. The destination choice is a plain if/else
-- two destinations are not architecture. Every exit prints one summary line, plus
row-level error detail to stderr when there were any, so a DAG task wrapping this
subprocess can carry counts and error codes into its own failure alert instead of
a bare exit code.

`--init-log` is the one-time (or post-reset) setup ceremony: create the flow's own
log table if it does not exist yet, then exit -- the daily run never creates or
alters a table. `--accept-empty-log` is the deliberate override for the one day a
real run against a genuinely empty table is expected (a first run, or right after
an admin reset); every other day, an empty log fails the run instead of silently
re-sending the full population. Daily DAG invocations pass neither flag.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from . import csv_destination, databricks_io, hubspot_destination, sent_log
from .config import load_flow_config
from .destinations import Destination
from .run import error_report_lines, execute_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diff a Databricks desired-state model against a destination"
    )
    parser.add_argument("--source", required=True, help="Flow name; selects RETL_FLOW_<NAME>_* config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--destination",
        choices=["hubspot_contacts", "csv"],
        help="Where to deliver the diff",
    )
    mode.add_argument(
        "--init-log",
        action="store_true",
        help="Create the flow's log table if it does not exist yet, then exit (setup only)",
    )
    parser.add_argument(
        "--accept-empty-log",
        action="store_true",
        help=(
            "Allow a run against a log table with no rows for this flow yet -- a deliberate "
            "first run or post-reset run only. Valid only with --destination."
        ),
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.accept_empty_log and args.init_log:
        parser.error("--accept-empty-log is valid only with --destination, not --init-log")
    return args


def _build_destination(name: str, env: dict[str, str]) -> Destination:
    if name == "hubspot_contacts":
        return hubspot_destination.HubSpotDestination(hubspot_destination.config_from_env(env))
    return csv_destination.CsvDestination(csv_destination.config_from_env(env))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    env = dict(os.environ)

    try:
        flow = load_flow_config(args.source, env)
        connection = databricks_io.connect(databricks_io.config_from_env(env))
        try:
            if args.init_log:
                created = sent_log.init_log_table(connection, flow.log_table, flow.flow_id)
                state = "created" if created else "already present"
                print(f"retl init-log flow={flow.flow_id} table={flow.log_table} {state}")
                return 0

            destination = _build_destination(args.destination, env)
            summary = execute_run(
                connection=connection,
                flow=flow,
                destination=destination,
                accept_empty_log=args.accept_empty_log,
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
