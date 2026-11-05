"""hsm, the merge job's command line.

The three commands are the design's: `run` merges the queue, `init-log` creates the log
table once, and `verify` re-checks a finished run against HubSpot. This change fixes the
argument surface and leaves the bodies to the steps that implement them, so the console
script, the image and their tests can exist now.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hsm", description="Merge duplicate HubSpot contacts from a Databricks relation"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Merge the pairs in the pair relation, one log row per pair")
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and classify the pairs, then merge nothing and write no log row",
    )

    commands.add_parser("init-log", help="Create the merge log table if it does not exist yet, then exit")

    verify = commands.add_parser("verify", help="Re-check the merged pairs of one run against HubSpot")
    verify.add_argument("--run-id", required=True, help="The run_id its log rows were stamped with")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    build_parser().parse_args(argv)
    raise SystemExit("hsm: this command is not wired up yet")


if __name__ == "__main__":
    raise SystemExit(main())
